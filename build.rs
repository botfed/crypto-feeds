fn main() {
    println!("cargo:rerun-if-changed=proto/");
    println!("cargo:rerun-if-changed=build.rs");

    let mut config = prost_build::Config::new();

    // Compile the proto files we need
    config
        .compile_protos(
            &[
                "proto/PublicAggreBookTickerV3Api.proto",
                "proto/MexcWrapper.proto",
            ],
            &["proto/"],
        )
        .unwrap();
}
