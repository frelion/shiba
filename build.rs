use std::env;
use std::path::PathBuf;
use std::process::Command;

fn pg_config(argument: &str) -> String {
    let executable = env::var_os("PGRX_PG_CONFIG_PATH")
        .or_else(|| env::var_os("PG_CONFIG"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("pg_config"));
    let output = Command::new(&executable)
        .arg(argument)
        .output()
        .unwrap_or_else(|error| panic!("failed to run {}: {error}", executable.display()));
    assert!(
        output.status.success(),
        "{} {argument} failed: {}",
        executable.display(),
        String::from_utf8_lossy(&output.stderr).trim()
    );
    String::from_utf8(output.stdout)
        .expect("pg_config output must be UTF-8")
        .trim()
        .to_owned()
}

fn main() {
    println!("cargo:rerun-if-env-changed=PGRX_PG_CONFIG_PATH");
    println!("cargo:rerun-if-env-changed=PG_CONFIG");

    let version = pg_config("--version");
    let major = version
        .strip_prefix("PostgreSQL ")
        .and_then(|version| version.split('.').next())
        .unwrap_or_default();
    assert!(
        matches!(major, "17" | "18"),
        "Shiba replication transport requires PostgreSQL 17 or 18, found {version}"
    );

    let configured_major = if env::var_os("CARGO_FEATURE_PG18").is_some() {
        "18"
    } else if env::var_os("CARGO_FEATURE_PG17").is_some() {
        "17"
    } else {
        ""
    };
    assert_eq!(
        major, configured_major,
        "the selected pgrx feature ({configured_major:?}) does not match pg_config ({version})"
    );

    println!("cargo:rustc-link-search=native={}", pg_config("--libdir"));
    println!("cargo:rustc-link-lib=pq");
}
