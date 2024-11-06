/*
 * Copyright 2024 Oxide Computer Company
 */

use std::collections::BTreeMap;

use serde::Serialize;

/*
 * Humility accepts a JSON file describing a list of probes, so that they can be
 * selected by name using the -t (target) option.  We write out such a file and
 * include the path in the the job environment (HUMILITY_ENVIRONMENT) so that it
 * can be used by humility commands invoked in the job.
 */
type EnvironmentFile = BTreeMap<String, Environment>;

#[derive(Serialize)]
pub struct Environment {
    pub description: String,
    pub probe: String,
    pub archive: String,
}

#[derive(Serialize)]
#[serde(untagged)]
pub enum Archive {
    Single(String),
    Slots(TwoSlots),
}

#[derive(Serialize)]
pub struct TwoSlots {
    imagea: String,
    imageb: String,
}
