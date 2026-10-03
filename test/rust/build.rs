fn main() -> Result<(), Box<dyn std::error::Error>> {
    prost_build::Config::new()
        .include_file("messages.rs")
        .compile_protos(
            &["../fixtures/system.proto", "../fixtures/rust.proto"],
            &["../fixtures", "../.."],
        )?;
    println!("cargo:rerun-if-changed=../fixtures/system.proto");
    println!("cargo:rerun-if-changed=../fixtures/rust.proto");
    println!("cargo:rerun-if-changed=../../proto/proprdb/options.proto");
    Ok(())
}
