mod prelude {
    pub(crate) use crate::App;
    pub use anyhow::{anyhow, Result};
    pub use dropshot::{
        endpoint, HttpError, HttpResponseCreated, HttpResponseDeleted,
        HttpResponseOk, HttpResponseUpdatedNoContent, Path as TypedPath,
        Query as TypedQuery, RequestContext, TypedBody, UntypedBody,
    };
    pub use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE};
    pub use hyper::StatusCode;
    pub use hyper::{Body, Response};
    pub use schemars::JsonSchema;
    pub use serde::{Deserialize, Serialize};
    pub use slog::{error, info, warn, Logger};
    pub use std::collections::HashMap;
    pub use std::io::{Read, Write};
    pub use std::result::Result as SResult;
    pub use std::str::FromStr;
    pub use std::sync::Arc;

    pub type DSResult<T> = std::result::Result<T, HttpError>;
}

pub mod gerrit;
