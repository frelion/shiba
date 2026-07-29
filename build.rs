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
    assert!(
        version.starts_with("PostgreSQL 17."),
        "Shiba replication transport requires PostgreSQL 17, found {version}"
    );

    println!("cargo:rustc-link-search=native={}", pg_config("--libdir"));
    println!("cargo:rustc-link-lib=pq");
}
