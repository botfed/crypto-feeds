#!/usr/bin/env python3
"""
Offline calibration of fair price engine structural parameters.

Estimates everything from a single lead_lag_capture run:
  - Fair price via cross-sectional median of exchange mids
  - Per-exchange noise variance (sigma_k^2) and bias (m_k)
  - GARCH alpha, beta via noise-corrected QMLE on median returns
  - initial_var and process_noise_floor derived from above

Only input: lead_lag.csv.gz from lead_lag_capture (one process, one capture).

Usage:
  # Capture (one terminal, 4+ hours, Ctrl-C to stop)
  cargo run --release --bin lead_lag_capture -- configs/config.yaml data/fp_calib/lead_lag.csv.gz

  # Calibrate
  python scripts/fp_calibrate.py --data-dir data/fp_calib --warmup-secs 300
"""

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import yaml
from scipy.optimize import minimize

# ── Constants ────────────────────────────────────────────────────────────────

CACHE_PATH = "/tmp/fair_price_cache.json"
CALIB_PATH = "configs/fp_calibrated.json"
BPS = 1e4


# ── Helpers ──────────────────────────────────────────────────────────────────


def discover_groups(config_path: str) -> list[tuple[str, list[tuple[str, str]]]]:
    """Replicate auto_discover_groups from Rust.

    Returns list of (base_asset, [(exchange, canonical_symbol), ...]) sorted by base.
    Only groups with >= 2 members are included.
    """
    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    base_map: dict[str, list[tuple[str, str]]] = {}

    for exchange, symbols in (cfg.get("spot") or {}).items():
        for sym in symbols:
            base = sym.split("_")[0]
            base_map.setdefault(base, []).append((exchange, sym))

    for exchange, symbols in (cfg.get("perp") or {}).items():
        for sym in symbols:
            base = sym.split("_")[0]
            base_map.setdefault(base, []).append((exchange, sym))

    USD_QUOTES = {"USD", "USDT", "USDC"}
    onchain = cfg.get("onchain") or {}
    for dex_name in ["aerodrome", "uniswap"]:
        dex_cfg = onchain.get(dex_name)
        if dex_cfg and "pools" in dex_cfg:
            for pool in dex_cfg["pools"]:
                parts = pool["symbol"].split("_")
                if len(parts) == 2:
                    base_map.setdefault(parts[0], []).append(
                        (dex_name, pool["symbol"])
                    )

    groups = []
    for base in sorted(base_map.keys()):
        members = base_map[base]
        if len(members) >= 2:
            groups.append((base, members))

    return groups


def acf_fft(x: np.ndarray, max_lag: int) -> np.ndarray:
    """Fast autocorrelation via FFT."""
    n = len(x)
    x = x - x.mean()
    nfft = 2 ** int(np.ceil(np.log2(2 * n)))
    fft_x = np.fft.fft(x, n=nfft)
    acf = np.fft.ifft(fft_x * np.conj(fft_x))[:n].real
    if acf[0] > 0:
        acf /= acf[0]
    return acf[: max_lag + 1]


# ── GARCH ────────────────────────────────────────────────────────────────────


def garch_nll(
    params: np.ndarray,
    r_list: list[float],
    R_noise: float,
    var_signal: float,
) -> float:
    """Noise-corrected GARCH(1,1) negative log-likelihood."""
    alpha, beta = params[0], params[1]
    if alpha <= 0 or beta <= 0 or alpha + beta >= 1:
        return 1e20

    omega = var_signal * max(1 - alpha - beta, 0.001)
    h = var_signal
    nll = 0.0

    for rt in r_list:
        v = h + R_noise
        if v <= 0:
            v = 1e-20
        nll += math.log(v) + rt * rt / v
        r2_est = rt * rt - R_noise
        if r2_est < 0:
            r2_est = 0.0
        h = omega + alpha * r2_est + beta * h
        if h < 1e-20:
            h = 1e-20

    return nll


def garch_filter(
    r: np.ndarray, alpha: float, beta: float, R_noise: float, var_signal: float
) -> np.ndarray:
    """Run GARCH filter forward, return h_t series."""
    omega = var_signal * max(1 - alpha - beta, 0.001)
    h = var_signal
    h_out = np.empty(len(r))
    for i in range(len(r)):
        h_out[i] = h
        r2_est = max(r[i] * r[i] - R_noise, 0.0)
        h = omega + alpha * r2_est + beta * h
        h = max(h, 1e-20)
    return h_out


# ── Per-group calibration ────────────────────────────────────────────────────


def calibrate_group(
    ll_base: pd.DataFrame, base: str
) -> tuple[dict[str, tuple[float, float]], dict | None, dict]:
    """Calibrate one pricing group entirely from lead_lag data.

    Returns:
        rulers: {exchange: (m_k, sigma_k_sq)}
        garch_params: {alpha, beta, initial_var, ...} or None
        diag: diagnostic data for plotting
    """
    # ── Pivot: rows=sample_ts, cols=exchange, values=log_mid ──
    pivot = ll_base.pivot_table(
        index="sample_ts",
        columns="exchange",
        values="log_mid",
        aggfunc="mean",  # average if multiple symbols per exchange
    )

    # Drop samples with < 2 exchanges
    valid = pivot.notna().sum(axis=1) >= 2
    pivot = pivot[valid].sort_index()

    if len(pivot) < 1000:
        print(f"  WARNING: {base} has only {len(pivot)} valid samples, skipping")
        return {}, None, {}

    n_exchanges = pivot.notna().sum(axis=1).median()
    print(f"    {len(pivot)} samples, {int(n_exchanges)} median exchanges")

    # ── Fair price: cross-sectional median ──
    y_hat = pivot.median(axis=1)

    # ── Rulers: per-exchange residuals ──
    rulers: dict[str, tuple[float, float]] = {}
    for exchange in pivot.columns:
        residuals = (pivot[exchange] - y_hat).dropna()
        if len(residuals) < 500:
            continue

        m_k = float(residuals.mean())

        # Spread correction: bid-ask bounce adds rel_spread^2/12 to variance
        ex_mask = ll_base["exchange"] == exchange
        median_rel_spread = float(ll_base.loc[ex_mask, "rel_spread"].median())
        spread_correction = median_rel_spread**2 / 12

        sigma_k_sq = float(residuals.var()) - spread_correction
        sigma_k_sq = max(sigma_k_sq, 1e-12)

        rulers[exchange] = (m_k, sigma_k_sq)
        print(
            f"    {base}/{exchange}: m_k={m_k * BPS:.3f} bps, "
            f"sigma_k={np.sqrt(sigma_k_sq) * BPS:.3f} bps "
            f"(spread_corr={np.sqrt(spread_correction) * BPS:.3f} bps)"
        )

    # ── Resample to engine frequency (100ms) and compute returns ──
    # The engine runs at 100ms ticks; estimate params at that frequency
    # so they are directly usable without scaling.
    ENGINE_INTERVAL_NS = 100_000_000  # 100ms
    ts = y_hat.index.values
    median_vals = y_hat.values

    # Auto-detect native sample interval
    native_dt = float(np.median(np.diff(ts)))
    native_ms = native_dt / 1e6
    ratio = max(1, round(ENGINE_INTERVAL_NS / native_dt))

    if ratio > 1:
        # Subsample: take every ratio-th sample (last value per 100ms bin)
        ts_100 = ts[::ratio]
        mv_100 = median_vals[::ratio]
        print(
            f"    Native interval: {native_ms:.1f}ms, "
            f"resampling {ratio}x -> {len(ts_100)} samples at ~100ms"
        )
    else:
        ts_100 = ts
        mv_100 = median_vals
        print(f"    Sample interval: {native_ms:.1f}ms, {len(ts_100)} samples")

    ts = ts_100
    dt = np.diff(ts)
    median_dt = float(np.median(dt))
    ok_dt = (dt > median_dt * 0.5) & (dt < median_dt * 2.0)
    r = np.diff(mv_100)
    r = r[ok_dt]
    sample_ms = median_dt / 1e6
    print(f"    {ok_dt.sum()}/{len(ok_dt)} valid returns at {sample_ms:.0f}ms")

    if len(r) < 5000:
        print(
            f"  WARNING: {base} has only {len(r)} valid returns, "
            f"need 5k+ for GARCH. Skipping."
        )
        return rulers, None, {}

    r = r - r.mean()  # demean

    # ── Noise correction via lag-1 autocovariance ──
    # median_t = true_price_t + noise_t (from median estimator)
    # r_t = signal_t + (noise_t - noise_{t-1})
    # cov(r_t, r_{t+1}) = -var(noise)   [iid noise]
    # R_noise = 2 * var(noise) = -2 * cov(r_t, r_{t+1})
    lag1_cov = float(np.mean(r[:-1] * r[1:])) - float(np.mean(r[:-1])) * float(
        np.mean(r[1:])
    )
    R_noise = max(-2.0 * lag1_cov, 0.0)

    var_total = float(np.var(r))
    var_signal = max(var_total - R_noise, 1e-12)

    print(
        f"    var_total={var_total:.2e}, R_noise={R_noise:.2e} "
        f"(autocov={lag1_cov:.2e}), var_signal={var_signal:.2e}"
    )

    if var_signal < 1e-14:
        print(f"  WARNING: var_signal too small for {base}, skipping GARCH")
        return rulers, None, {}

    # ── GARCH QMLE: grid search + Nelder-Mead ──
    r_list = r.tolist()

    best_nll = 1e30
    best_ab = (0.06, 0.94)
    for a in np.arange(0.01, 0.16, 0.01):
        for b in np.arange(0.80, 0.99, 0.01):
            if a + b >= 0.9999:
                continue
            nll = garch_nll(np.array([a, b]), r_list, R_noise, var_signal)
            if nll < best_nll:
                best_nll = nll
                best_ab = (float(a), float(b))

    print(
        f"    Grid best: alpha={best_ab[0]:.3f}, beta={best_ab[1]:.3f}, "
        f"NLL={best_nll:.2f}"
    )

    result = minimize(
        garch_nll,
        x0=np.array(best_ab),
        args=(r_list, R_noise, var_signal),
        method="Nelder-Mead",
        options={"maxiter": 500, "xatol": 1e-4, "fatol": 1.0},
    )

    alpha_est = float(np.clip(result.x[0], 0.005, 0.25))
    beta_est = float(np.clip(result.x[1], 0.70, 0.995))
    if alpha_est + beta_est >= 0.9999:
        beta_est = 0.9999 - alpha_est

    print(
        f"    Optimized: alpha={alpha_est:.4f}, beta={beta_est:.4f}, "
        f"NLL={result.fun:.2f}"
    )

    # ── vol_halflife from ACF of noise-corrected squared returns ──
    r2_corrected = np.maximum(r**2 - R_noise, 0.0)
    vol_halflife = 5000  # default

    if len(r2_corrected) > 20000:
        acf = acf_fft(r2_corrected, max_lag=20000)
        halflife_lags = np.where(acf[1:] < 0.5)[0]
        if len(halflife_lags) > 0:
            vol_halflife = int(halflife_lags[0] + 1)
            print(
                f"    ACF halflife: {vol_halflife} snaps "
                f"({vol_halflife * sample_ms / 1000:.1f}s)"
            )
        else:
            print(
                "    ACF did not decay to 0.5 within 20k lags, "
                "keeping default vol_halflife=5000"
            )
    else:
        print(
            f"    Insufficient data for ACF ({len(r2_corrected)} < 20k), "
            "keeping default vol_halflife=5000"
        )

    # ── GARCH h time series for diagnostics ──
    h_series = garch_filter(r, alpha_est, beta_est, R_noise, var_signal)

    garch_params = {
        "alpha": round(alpha_est, 6),
        "beta": round(beta_est, 6),
        "initial_var": float(var_signal),
        "process_noise_floor": float(var_signal * 0.01),
        "vol_halflife": vol_halflife,
    }

    diag = {
        "r": r,
        "r2_corrected": r2_corrected,
        "h_series": h_series,
        "R_noise": R_noise,
        "ts": ts[1:][ok_dt],  # timestamps matching r
    }

    return rulers, garch_params, diag


# ── Cache ────────────────────────────────────────────────────────────────────


def write_ruler_cache(
    rulers: dict[str, dict[str, tuple[float, float]]],
    cache_path: str = CACHE_PATH,
) -> int:
    """Update existing cache with cross-sectional ruler estimates.

    Reads existing cache to preserve symbol_id key mapping, updates values
    where (group, exchange) matches.  Returns number of entries updated.
    """
    existing: dict = {}
    try:
        with open(cache_path) as f:
            existing = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        print(
            f"  No existing cache at {cache_path}. "
            "Run the engine once first to establish key mapping."
        )
        return 0

    updated = 0
    new_cache = dict(existing)

    for key in list(new_cache.keys()):
        parts = key.split("/")
        if len(parts) != 3:
            continue
        group, exchange, _ = parts
        if group in rulers and exchange in rulers[group]:
            m_k, sigma_k_sq = rulers[group][exchange]
            new_cache[key] = [m_k, sigma_k_sq]
            updated += 1

    if updated > 0:
        with open(cache_path, "w") as f:
            json.dump(new_cache, f, indent=2)
        print(f"  Updated {updated} entries in {cache_path}")

    return updated


# ── Plotting ─────────────────────────────────────────────────────────────────


def make_plots(
    group_diags: dict[str, dict],
    output_dir: Path,
):
    """Generate calibration diagnostic plots from lead_lag-derived data."""
    from scipy import stats as sp_stats

    plot_dir = output_dir / "calib_plots"
    plot_dir.mkdir(exist_ok=True)

    for gname, diag in group_diags.items():
        r = diag.get("r")
        if r is None or len(r) < 100:
            continue

        ts = diag["ts"]
        t_min = (ts - ts[0]) / 1e9 / 60  # minutes

        fig, axes = plt.subplots(2, 2, figsize=(14, 10))

        # 1. Return distribution
        r_bps = r * BPS
        std_r = np.std(r_bps)
        axes[0, 0].hist(r_bps, bins=200, density=True, alpha=0.7, label="returns")
        if std_r > 0:
            x = np.linspace(-5 * std_r, 5 * std_r, 200)
            axes[0, 0].plot(
                x, sp_stats.norm.pdf(x, 0, std_r), "r-", lw=2, label="Normal"
            )
            axes[0, 0].set_xlim(-5 * std_r, 5 * std_r)
        axes[0, 0].set_xlabel("Return (bps)")
        axes[0, 0].set_title(f"{gname}: Median Return Distribution")
        axes[0, 0].legend()

        # 2. GARCH h time series
        h = diag["h_series"]
        axes[0, 1].plot(t_min, np.sqrt(h) * BPS, alpha=0.8, label="sqrt(h)")
        axes[0, 1].set_xlabel("Time (min)")
        axes[0, 1].set_ylabel("Vol (bps/tick)")
        axes[0, 1].set_title(f"{gname}: Estimated GARCH Volatility")
        axes[0, 1].legend()

        # 3. QQ plot of returns
        sp_stats.probplot(r_bps, dist="norm", plot=axes[1, 0])
        axes[1, 0].set_title(f"{gname}: QQ Plot")

        # 4. ACF of noise-corrected squared returns
        r2c = diag["r2_corrected"]
        if len(r2c) > 1000:
            max_lag = min(500, len(r2c) - 1)
            acf = acf_fft(r2c, max_lag)
            axes[1, 1].plot(np.arange(1, max_lag + 1), acf[1:], alpha=0.8)
            axes[1, 1].axhline(0.5, color="r", ls="--", alpha=0.5, label="0.5")
            axes[1, 1].axhline(0, color="k", lw=0.5)
            axes[1, 1].set_xlabel("Lag (ticks)")
            axes[1, 1].set_ylabel("ACF")
            axes[1, 1].set_title(f"{gname}: ACF of r2_corrected")
            axes[1, 1].legend()

        fig.suptitle(f"Calibration Diagnostics: {gname}", fontsize=14)
        fig.tight_layout()
        fig.savefig(plot_dir / f"{gname}_calibration.png", dpi=100)
        plt.close(fig)

    print(f"  Plots saved to {plot_dir}/")


# ── Main ─────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Offline calibration of fair price engine parameters"
    )
    parser.add_argument(
        "--data-dir",
        default="data/fp_calib",
        help="Directory containing lead_lag.csv.gz",
    )
    parser.add_argument(
        "--warmup-secs",
        type=int,
        default=300,
        help="Warmup period to strip (seconds)",
    )
    parser.add_argument(
        "--config",
        default="configs/config.yaml",
        help="Path to config.yaml for group discovery",
    )
    parser.add_argument(
        "--no-plots", action="store_true", help="Skip plot generation"
    )
    parser.add_argument(
        "--no-cache", action="store_true", help="Don't update ruler cache"
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    warmup_ns = args.warmup_secs * 1_000_000_000

    print(
        f"fp_calibrate: data_dir={data_dir}, warmup={args.warmup_secs}s, "
        f"config={args.config}"
    )

    # ── Load ──
    print("\n== Loading data ==")
    groups = discover_groups(args.config)
    print(f"  Groups: {', '.join(name for name, _ in groups)}")

    ll_path = data_dir / "lead_lag.csv.gz"
    if not ll_path.exists():
        print(f"ERROR: {ll_path} not found")
        sys.exit(1)

    ll = pd.read_csv(ll_path)
    t0 = ll["sample_ts"].min()
    ll = ll[ll["sample_ts"] >= t0 + warmup_ns].copy()
    print(f"  lead_lag: {len(ll)} rows after warmup strip")

    if len(ll) == 0:
        print("ERROR: No data after warmup stripping.")
        sys.exit(1)

    # Precompute columns used by all groups
    ll = ll.dropna(subset=["bid", "ask"])
    ll = ll[(ll["bid"] > 0) & (ll["ask"] > 0)]
    ll["log_mid"] = np.log((ll["bid"] + ll["ask"]) / 2)
    ll["mid"] = (ll["bid"] + ll["ask"]) / 2
    ll["rel_spread"] = (ll["ask"] - ll["bid"]) / ll["mid"]
    # canonical_symbol format: "SPOT-BTC-USDT" or "PERP-ETH-USD" -> base = part[1]
    # Also handle legacy "BTC_USDT" format -> base = part[0]
    def extract_base(sym: str) -> str:
        if "-" in sym:
            return sym.split("-")[1]
        return sym.split("_")[0]

    ll["base"] = ll["canonical_symbol"].map(extract_base)

    duration_s = (ll["sample_ts"].max() - ll["sample_ts"].min()) / 1e9
    duration_h = duration_s / 3600
    print(f"  Duration: {duration_h:.1f} hours ({duration_s:.0f}s)")

    # ── Calibrate each group ──
    print("\n== Calibrating ==")
    calibrated: dict = {
        "calibrated_at": datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
        "capture_hours": round(duration_h, 1),
        "groups": {},
    }
    all_rulers: dict[str, dict[str, tuple[float, float]]] = {}
    group_diags: dict[str, dict] = {}

    for gi, (gname, _) in enumerate(groups):
        print(f"\n  Group {gi}: {gname}")
        ll_base = ll[ll["base"] == gname]
        if len(ll_base) == 0:
            print(f"    No data for {gname}")
            continue

        rulers, garch_params, diag = calibrate_group(ll_base, gname)

        if rulers:
            all_rulers[gname] = rulers
        if garch_params is not None:
            calibrated["groups"][gname] = garch_params
            print(
                f"    -> alpha={garch_params['alpha']:.4f}, "
                f"beta={garch_params['beta']:.4f}, "
                f"initial_var={garch_params['initial_var']:.2e}, "
                f"vol_halflife={garch_params['vol_halflife']}"
            )
        if diag:
            group_diags[gname] = diag

    # ── Write outputs ──
    calib_path = Path(CALIB_PATH)
    calib_path.parent.mkdir(exist_ok=True)
    with open(calib_path, "w") as f:
        json.dump(calibrated, f, indent=2)
    print(f"\n  Wrote structural params to {calib_path}")

    if not args.no_cache and all_rulers:
        write_ruler_cache(all_rulers)

    # ── Plots ──
    if not args.no_plots and group_diags:
        print("\n== Generating plots ==")
        make_plots(group_diags, data_dir)

    # ── Summary ──
    print(f"\n{'=' * 60}")
    print("  CALIBRATION SUMMARY")
    print(f"{'=' * 60}")
    print(f"  Data: {duration_h:.1f} hours from lead_lag.csv.gz")
    print(f"  Groups calibrated: {len(calibrated['groups'])}")
    for gname, params in calibrated["groups"].items():
        print(
            f"    {gname}: alpha={params['alpha']:.4f}, beta={params['beta']:.4f}, "
            f"sigma_unc={np.sqrt(params['initial_var']) * BPS:.2f} bps, "
            f"halflife={params['vol_halflife']}"
        )
    print(f"\n  Outputs:")
    print(f"    Structural: {CALIB_PATH}")
    if not args.no_cache and all_rulers:
        print(f"    Rulers:     {CACHE_PATH} (if engine was run previously)")
    print(f"\n  Next steps:")
    print(
        "    1. Start the engine (loads calibrated params automatically)"
    )
    print("    2. Rulers converge via online recalibration within ~5 min")
    print(
        "    3. Run fp_validate.py after 2+ hours to verify improvement"
    )


if __name__ == "__main__":
    main()
