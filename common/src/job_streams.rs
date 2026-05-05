/*
 * Copyright 2026 Oxide Computer Company
 */

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobStream {
    Agent,
    Bg { name: String },
    BgStderr { name: String },
    BgStdout { name: String },
    Console,
    Control,
    Diag { name: String },
    Error,
    Panic,
    Post { name: String },
    PostStderr { name: String },
    PostStdout { name: String },
    Stderr,
    Stdout,
    Task,
    Worker,
    Unknown(String),
}

impl JobStream {
    pub fn from_str(stream: &str) -> JobStream {
        let parts = stream.split('.').collect::<Vec<_>>();
        match parts.as_slice() {
            ["agent"] => JobStream::Agent,
            ["bg", name] => JobStream::Bg { name: name.to_string() },
            ["bg", name, "stdout"] => {
                JobStream::BgStdout { name: name.to_string() }
            }
            ["bg", name, "stderr"] => {
                JobStream::BgStderr { name: name.to_string() }
            }
            ["console"] => JobStream::Console,
            ["control"] => JobStream::Control,
            ["diag", name] => JobStream::Diag { name: name.to_string() },
            ["error"] => JobStream::Error,
            ["panic"] => JobStream::Panic,
            ["post", name] => JobStream::Post { name: name.to_string() },
            ["post", name, "stderr"] => {
                JobStream::PostStderr { name: name.to_string() }
            }
            ["post", name, "stdout"] => {
                JobStream::PostStdout { name: name.to_string() }
            }
            ["stderr"] => JobStream::Stderr,
            ["stdout"] => JobStream::Stdout,
            ["task"] => JobStream::Task,
            ["worker"] => JobStream::Worker,
            _ => JobStream::Unknown(stream.to_string()),
        }
    }

    pub fn is_output(&self) -> bool {
        match self {
            JobStream::BgStderr { .. }
            | JobStream::BgStdout { .. }
            | JobStream::PostStderr { .. }
            | JobStream::PostStdout { .. }
            | JobStream::Stderr
            | JobStream::Stdout => true,
            JobStream::Agent
            | JobStream::Bg { .. }
            | JobStream::Console
            | JobStream::Control
            | JobStream::Diag { .. }
            | JobStream::Error
            | JobStream::Panic
            | JobStream::Post { .. }
            | JobStream::Task
            | JobStream::Worker
            | JobStream::Unknown(_) => false,
        }
    }
}

impl std::fmt::Display for JobStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobStream::Agent => write!(f, "agent"),
            JobStream::Bg { name } => write!(f, "bg.{name}"),
            JobStream::BgStderr { name } => write!(f, "bg.{name}.stderr"),
            JobStream::BgStdout { name } => write!(f, "bg.{name}.stdout"),
            JobStream::Console => f.write_str("console"),
            JobStream::Control => f.write_str("control"),
            JobStream::Diag { name } => write!(f, "diag.{name}"),
            JobStream::Error => f.write_str("error"),
            JobStream::Panic => f.write_str("panic"),
            JobStream::Post { name } => write!(f, "post.{name}"),
            JobStream::PostStderr { name } => write!(f, "post.{name}.stderr"),
            JobStream::PostStdout { name } => write!(f, "post.{name}.stdout"),
            JobStream::Stderr => f.write_str("stderr"),
            JobStream::Stdout => f.write_str("stdout"),
            JobStream::Task => f.write_str("task"),
            JobStream::Worker => f.write_str("worker"),
            JobStream::Unknown(s) => f.write_str(s),
        }
    }
}
