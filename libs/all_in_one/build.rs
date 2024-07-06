extern crate cbindgen;

use cbindgen::{Config, Language};
use std::env;
use std::error::Error;
use std::path::PathBuf;

/// Generates the headers for the go program.
fn main() -> Result<(), Box<dyn Error>> {
    let crate_dir = env::var("CARGO_MANIFEST_DIR")?;
    let package_name = env::var("CARGO_PKG_NAME")?;

    // We export the header file to build/{package_name}.h
    let output_file = PathBuf::from("../../internal/pkg/all-in-one")
        .join(format!("{}.h", package_name))
        .display()
        .to_string();
    let mut config = Config::default();
    config.language = Language::C;

    cbindgen::generate_with_config(crate_dir, config)?.write_to_file(output_file);

    Ok(())
}
