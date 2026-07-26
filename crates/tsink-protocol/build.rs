use std::error::Error;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn Error>> {
    let proto_root = PathBuf::from("proto");
    let prometheus_proto_root = proto_root.join("prometheus");
    let otlp_proto_root = proto_root.join("opentelemetry");

    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed={}", proto_root.display());

    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);

    prost_build::Config::new().compile_protos(
        &[
            prometheus_proto_root.join("remote.proto"),
            prometheus_proto_root.join("types.proto"),
        ],
        &[prometheus_proto_root, proto_root.clone()],
    )?;

    prost_build::Config::new().compile_protos(
        &[otlp_proto_root.join("proto/collector/metrics/v1/metrics_service.proto")],
        &[proto_root],
    )?;

    Ok(())
}
