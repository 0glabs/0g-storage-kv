use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=../zerog-storage-rust");

    let status = Command::new("cargo")
        .current_dir("../zerog-storage-rust")
        .args(vec!["build", "--release"])
        .status()
        .unwrap();

    println!("build zerog-storage-rust with status {}", status);
}
