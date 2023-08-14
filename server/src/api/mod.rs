/*
 * Copyright 2021 Oxide Computer Company
 */

mod prelude {
    pub(crate) use crate::{db, unauth_response, Central, MakeInternalError};
    pub use anyhow::{anyhow, Result};
    pub use buildomat_types::metadata;
    pub use chrono::prelude::*;
    pub use dropshot::{
        endpoint, HttpError, HttpResponseCreated, HttpResponseDeleted,
        HttpResponseOk, HttpResponseUpdatedNoContent, Path as TypedPath,
        Query as TypedQuery, RequestContext, TypedBody, UntypedBody,
    };
    pub use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE};
    pub use hyper::StatusCode;
    pub use hyper::{Body, Response};
    pub use hyper_staticfile::FileBytesStream;
    pub use io::{Read, Write};
    pub use rusty_ulid::Ulid;
    pub use schemars::JsonSchema;
    pub use serde::{Deserialize, Serialize};
    pub use slog::{error, info, warn, Logger};
    pub use std::collections::HashMap;
    pub use std::fs;
    pub use std::io;
    pub use std::result::Result as SResult;
    pub use std::str::FromStr;
    pub use std::sync::Arc;

    pub type DSResult<T> = std::result::Result<T, HttpError>;
}

pub mod admin;
pub mod factory;
pub mod public;
pub mod user;
pub mod worker;
