//! Runtime heap-allocation enforcement for HFT hot paths.
//!
//! Provides `HftAllocator` — a `#[global_allocator]` wrapper that intercepts
//! every `malloc`. When armed, any allocation either aborts the process
//! (deny mode) or increments a counter (count mode).
//!
//! # Usage
//!
//! In your MM binary's `main.rs`:
//!
//! ```ignore
//! use crypto_feeds::hft::alloc_guard::HftAllocator;
//!
//! #[global_allocator]
//! static ALLOC: HftAllocator = HftAllocator;
//!
//! fn main() {
//!     // ... setup, TLS connect, warmup ...
//!
//!     // Arm: any allocation from here is a bug
//!     crypto_feeds::hft::alloc_guard::arm();
//!
//!     loop {
//!         engine.poll_once();
//!         // MM logic ...
//!     }
//!
//!     // Or use the RAII guard:
//!     {
//!         let _guard = crypto_feeds::hft::alloc_guard::DenyAllocGuard::new();
//!         // ... zero-alloc region ...
//!     } // disarmed on drop
//! }
//! ```

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Whether the deny-allocator is currently armed.
static ARMED: AtomicBool = AtomicBool::new(false);

/// Number of allocations that occurred while armed.
/// Incremented regardless of mode (abort or count).
static VIOLATION_COUNT: AtomicU64 = AtomicU64::new(0);

/// If true, abort on violation. If false, just count.
static ABORT_ON_VIOLATION: AtomicBool = AtomicBool::new(true);

/// Custom global allocator for HFT hot-path enforcement.
///
/// When disarmed (default): passes all allocations to the system allocator.
/// When armed: detects allocations and either aborts or counts them.
///
/// Deallocation always passes through — freeing memory is fine on the hot path.
pub struct HftAllocator;

unsafe impl GlobalAlloc for HftAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if ARMED.load(Ordering::Relaxed) {
            VIOLATION_COUNT.fetch_add(1, Ordering::Relaxed);
            if ABORT_ON_VIOLATION.load(Ordering::Relaxed) {
                // Write to stderr without allocating (raw fd write)
                #[cfg(unix)]
                {
                    let msg = b"FATAL: heap alloc in deny-alloc region\n";
                    unsafe {
                        libc::write(2, msg.as_ptr() as *const _, msg.len());
                    }
                }
                std::process::abort();
            }
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if ARMED.load(Ordering::Relaxed) {
            VIOLATION_COUNT.fetch_add(1, Ordering::Relaxed);
            if ABORT_ON_VIOLATION.load(Ordering::Relaxed) {
                #[cfg(unix)]
                {
                    let msg = b"FATAL: heap alloc_zeroed in deny-alloc region\n";
                    unsafe {
                        libc::write(2, msg.as_ptr() as *const _, msg.len());
                    }
                }
                std::process::abort();
            }
        }
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if ARMED.load(Ordering::Relaxed) {
            VIOLATION_COUNT.fetch_add(1, Ordering::Relaxed);
            if ABORT_ON_VIOLATION.load(Ordering::Relaxed) {
                #[cfg(unix)]
                {
                    let msg = b"FATAL: heap realloc in deny-alloc region\n";
                    unsafe {
                        libc::write(2, msg.as_ptr() as *const _, msg.len());
                    }
                }
                std::process::abort();
            }
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

/// Arm the deny-allocator. Any heap allocation after this call is a violation.
#[inline]
pub fn arm() {
    ARMED.store(true, Ordering::Release);
}

/// Disarm the deny-allocator. Allocations are allowed again.
#[inline]
pub fn disarm() {
    ARMED.store(false, Ordering::Release);
}

/// Check if the deny-allocator is currently armed.
#[inline]
pub fn is_armed() -> bool {
    ARMED.load(Ordering::Relaxed)
}

/// Number of allocation violations detected while armed.
#[inline]
pub fn violation_count() -> u64 {
    VIOLATION_COUNT.load(Ordering::Relaxed)
}

/// Reset the violation counter to zero.
#[inline]
pub fn reset_violations() {
    VIOLATION_COUNT.store(0, Ordering::Relaxed);
}

/// Set whether violations abort the process (true) or just count (false).
/// Default: true (abort).
#[inline]
pub fn set_abort_on_violation(abort: bool) {
    ABORT_ON_VIOLATION.store(abort, Ordering::Relaxed);
}

/// RAII guard that arms the deny-allocator on construction and disarms on drop.
pub struct DenyAllocGuard {
    _private: (),
}

impl DenyAllocGuard {
    /// Arm the deny-allocator. Returns a guard that disarms on drop.
    #[inline]
    pub fn new() -> Self {
        arm();
        Self { _private: () }
    }
}

impl Drop for DenyAllocGuard {
    #[inline]
    fn drop(&mut self) {
        disarm();
    }
}
