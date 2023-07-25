
use super::super::prelude::*;
use super::parse_output_rule;

#[test]
fn test_parse_output_rule() -> Result<()> {
    let cases = vec![
        (
            "/var/log/*.log",
            db::CreateOutputRule {
                rule: "/var/log/*.log".into(),
                ignore: false,
                size_change_ok: false,
                require_match: false,
            },
        ),
        (
            "!/var/log/*.log",
            db::CreateOutputRule {
                rule: "/var/log/*.log".into(),
                ignore: true,
                size_change_ok: false,
                require_match: false,
            },
        ),
        (
            "=/var/log/*.log",
            db::CreateOutputRule {
                rule: "/var/log/*.log".into(),
                ignore: false,
                size_change_ok: false,
                require_match: true,
            },
        ),
        (
            "%/var/log/*.log",
            db::CreateOutputRule {
                rule: "/var/log/*.log".into(),
                ignore: false,
                size_change_ok: true,
                require_match: false,
            },
        ),
        (
            "=%/var/log/*.log",
            db::CreateOutputRule {
                rule: "/var/log/*.log".into(),
                ignore: false,
                size_change_ok: true,
                require_match: true,
            },
        ),
        (
            "%=/var/log/*.log",
            db::CreateOutputRule {
                rule: "/var/log/*.log".into(),
                ignore: false,
                size_change_ok: true,
                require_match: true,
            },
        ),
    ];

    for (rule, want) in cases {
        println!("case {:?} -> {:?}", rule, want);
        let got = parse_output_rule(rule)?;
        assert_eq!(got, want);
    }

    Ok(())
}

#[test]
fn test_parse_output_rule_failures() -> Result<()> {
    let cases = vec![
        "",
        "target/some/file",
        "!var/log/*.log",
        "%var/log/*.log",
        "=var/log/*.log",
        "!!/var/log/*.log",
        "!=/var/log/*.log",
        "!%/var/log/*.log",
        "%!/var/log/*.log",
        "=!/var/log/*.log",
        "==/var/log/*.log",
        "%%/var/log/*.log",
        "=%=/var/log/*.log",
        "%=%/var/log/*.log",
        "=%!/var/log/*.log",
        "%=!/var/log/*.log",
    ];

    for should_fail in cases {
        println!();
        println!("should fail {:?}", should_fail);
        match parse_output_rule(should_fail) {
            Err(e) => println!("  yes, fail! {:?}", e.external_message),
            Ok(res) => panic!("  wanted failure, got {:?}", res),
        }
    }

    Ok(())
}
