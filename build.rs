fn main() {
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
