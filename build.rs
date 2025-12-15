fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use a vendored protoc so building this project doesn't require a system protoc install.
    let protoc_path = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc_path);

    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&["proto/s3.proto"], &["proto"])?;

    println!("cargo:rerun-if-changed=proto/s3.proto");
    Ok(())
}
