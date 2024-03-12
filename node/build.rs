use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=../0g-storage-node");

    let status = Command::new("cargo")
        .current_dir("../0g-storage-node")
        .args(vec!["build", "--release"])
        .status()
        .unwrap();

    println!("build 0g-storage-node with status {}", status);
}
