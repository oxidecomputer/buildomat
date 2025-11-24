/*
 * Copyright 2025 Oxide Computer Company
 */

mod prelude {
    pub(crate) use crate::{Central, MakeInternalError, db, unauth_response};
    pub use anyhow::Result;
    pub use buildomat_download::{PotentialRange, RequestContextEx};
    pub use buildomat_sse::{HeaderMapEx, ServerSentEvents};
    pub use buildomat_types::metadata;
    pub use chrono::prelude::*;
    pub use dropshot::{
        Body, ClientErrorStatusCode, HttpError, HttpResponseCreated,
        HttpResponseDeleted, HttpResponseOk, HttpResponseUpdatedNoContent,
        PaginationParams, Path as TypedPath, Query as TypedQuery,
        RequestContext, ResultsPage, TypedBody, UntypedBody, WhichPage,
        endpoint,
    };
    pub use futures::TryStreamExt;
    pub use hyper::Response;
    pub use rusty_ulid::Ulid;
    pub use schemars::JsonSchema;
    pub use serde::{Deserialize, Serialize};
    pub use slog::{Logger, error, info, o, warn};
    pub use std::collections::HashMap;
    pub use std::str::FromStr;
    pub use std::sync::Arc;

    pub type DSResult<T> = std::result::Result<T, HttpError>;
}

pub mod admin;
pub mod factory;
pub mod public;
pub mod user;
pub mod worker;

pub(crate) use prelude::DSResult;
