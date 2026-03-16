use std::path::PathBuf;

use crate::config::*;

/// Minimal valid configuration for tests
const MINIMAL_CONFIG: &str = r#"
    [general]
    baseurl = "http://127.0.0.1:9979"

    [factory]
    token = "test-token"

    [execution]
    command = "test-runner"
    job_dir = "/var/lib/buildomat/jobs"

    [target.default]
"#;

#[test]
fn config_parse_minimal() {
    let config: ConfigFile = toml::from_str(MINIMAL_CONFIG).unwrap();

    assert_eq!(config.general.baseurl, "http://127.0.0.1:9979");
    assert_eq!(config.factory.token, "test-token");
    assert_eq!(config.execution.command, "test-runner");
    assert!(config.execution.args.is_empty());
    assert_eq!(
        config.execution.job_dir,
        PathBuf::from("/var/lib/buildomat/jobs")
    );
    assert!(config.target.contains_key("default"));
    assert_eq!(config.target.len(), 1);
}

#[test]
fn config_parse_with_args() {
    let config: ConfigFile = toml::from_str(
        r#"
        [general]
        baseurl = "http://127.0.0.1:9979"

        [factory]
        token = "test-token"

        [execution]
        command = "test-runner"
        args = ["ci-job", "--verbose"]
        job_dir = "/var/lib/buildomat/jobs"

        [target.default]
        "#,
    )
    .unwrap();

    assert_eq!(config.execution.args, vec!["ci-job", "--verbose"]);
}

#[test]
fn config_parse_multiple_targets() {
    let config: ConfigFile = toml::from_str(
        r#"
        [general]
        baseurl = "http://127.0.0.1:9979"

        [factory]
        token = "test-token"

        [execution]
        command = "test-runner"
        job_dir = "/var/lib/buildomat/jobs"

        [target.hwci-grapefruit]
        [target.hwci-gimlet]
        "#,
    )
    .unwrap();

    assert_eq!(config.target.len(), 2);
    assert!(config.target.contains_key("hwci-grapefruit"));
    assert!(config.target.contains_key("hwci-gimlet"));
}

#[test]
fn config_requires_execution_section() {
    let result: Result<ConfigFile, _> = toml::from_str(
        r#"
        [general]
        baseurl = "http://127.0.0.1:9979"

        [factory]
        token = "test-token"

        [target.default]
        "#,
    );

    assert!(result.is_err());
}

#[test]
fn config_requires_execution_command() {
    let result: Result<ConfigFile, _> = toml::from_str(
        r#"
        [general]
        baseurl = "http://127.0.0.1:9979"

        [factory]
        token = "test-token"

        [execution]
        job_dir = "/var/lib/buildomat/jobs"

        [target.default]
        "#,
    );

    assert!(result.is_err());
}

#[test]
fn config_rejects_unknown_fields() {
    let result: Result<ConfigFile, _> = toml::from_str(
        r#"
        [general]
        baseurl = "http://127.0.0.1:9979"
        unknown_field = "bad"

        [factory]
        token = "test-token"

        [execution]
        command = "test-runner"
        job_dir = "/var/lib/buildomat/jobs"

        [target.default]
        "#,
    );

    assert!(result.is_err());
}

#[test]
fn config_rejects_unknown_execution_fields() {
    let result: Result<ConfigFile, _> = toml::from_str(
        r#"
        [general]
        baseurl = "http://127.0.0.1:9979"

        [factory]
        token = "test-token"

        [execution]
        command = "test-runner"
        job_dir = "/var/lib/buildomat/jobs"
        unknown_field = "bad"

        [target.default]
        "#,
    );

    assert!(result.is_err());
}

#[test]
fn config_target_matching() {
    let config: ConfigFile = toml::from_str(
        r#"
        [general]
        baseurl = "http://127.0.0.1:9979"

        [factory]
        token = "test-token"

        [execution]
        command = "test-runner"
        job_dir = "/var/lib/buildomat/jobs"

        [target.hwci-grapefruit]
        [target.hwci-gimlet]
        "#,
    )
    .unwrap();

    // Factory should accept jobs for its configured targets
    assert!(config.target.contains_key("hwci-grapefruit"));
    assert!(config.target.contains_key("hwci-gimlet"));

    // Factory should NOT accept jobs for other targets
    assert!(!config.target.contains_key("helios"));
    assert!(!config.target.contains_key("default"));
}
