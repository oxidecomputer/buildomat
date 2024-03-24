/*
 * Copyright 2024 Oxide Computer Company
 */

mod sublude {
    pub(crate) use crate::stopwatch::Stopwatch;
    pub(crate) use crate::PotentialRange;
    pub(crate) use crate::{
        bad_range_response, make_get_response, make_head_response,
    };
}

pub mod file;
pub mod s3;
pub mod url;
