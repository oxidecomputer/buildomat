use super::schema::*;
use anyhow::{bail, Result};
use diesel::deserialize::FromSql;
use diesel::serialize::ToSql;
use serde_with::{DeserializeFromStr, SerializeDisplay};
use std::str::FromStr;

pub use buildomat_database::old::{Dictionary, IsoDate};
use buildomat_database::{integer_new_type, sql_for_enum};

integer_new_type!(InstanceSeq, u64, i64, BigInt, diesel::sql_types::BigInt);
integer_new_type!(EventSeq, u64, i64, BigInt, diesel::sql_types::BigInt);

#[derive(Clone, Debug, Queryable, Insertable, Identifiable)]
#[diesel(table_name = instance)]
#[diesel(primary_key(nodename, seq))]
pub struct Instance {
    pub nodename: String,
    pub seq: InstanceSeq,
    pub worker: String,
    pub target: String,
    pub state: InstanceState,
    pub key: String,
    pub bootstrap: String,
    pub flushed: bool,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    DeserializeFromStr,
    SerializeDisplay,
    FromSqlRow,
    diesel::expression::AsExpression,
)]
#[diesel(sql_type = diesel::sql_types::Text)]
pub enum InstanceState {
    /**
     * The instance has been created.  We will instruct iPXE to download the
     * boot media (kernel and ramdisk) as well as our postboot script.  The
     * postboot script will signal to the factory once the OS has started up,
     * moving us to the "Booted" state.
     */
    Preboot,

    /**
     * The OS has booted to the point where our postboot script signalled to us.
     * If we get another request for the iPXE scripts, we assume some
     * catastrophe has befallen the host and begin destroying the instance.
     */
    Booted,

    /**
     * Destruction of the instance has been requested for whatever reason.  We
     * need to reset the machine, and upload any final status records to the
     * central server.
     */
    Destroying,

    /**
     * This instance has come to rest.
     */
    Destroyed,
}
sql_for_enum!(InstanceState);

impl FromStr for InstanceState {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "preboot" => InstanceState::Preboot,
            "booted" => InstanceState::Booted,
            "destroying" => InstanceState::Destroying,
            "destroyed" => InstanceState::Destroyed,
            x => bail!("unknown user type: {:?}", x),
        })
    }
}

impl std::fmt::Display for InstanceState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use InstanceState::*;

        write!(
            f,
            "{}",
            match self {
                Preboot => "preboot",
                Booted => "booted",
                Destroying => "destroying",
                Destroyed => "destroyed",
            }
        )
    }
}

impl Instance {
    pub fn id(&self) -> String {
        format!("{}/{}", self.nodename, self.seq.0)
    }

    /**
     * This instance has been completely destroyed.  The host has been reset and
     * is holding at iPXE again.  All remaining state has been forwarded to the
     * core server.
     */
    pub fn destroyed(&self) -> bool {
        matches!(self.state, InstanceState::Destroyed)
    }

    pub fn target(&self) -> &str {
        &self.target
    }

    /**
     * This instance has been created but the host has not yet booted.
     */
    pub fn is_preboot(&self) -> bool {
        matches!(self.state, InstanceState::Preboot)
    }

    /**
     * This instance has been marked for destruction but has not yet been
     * destroyed.  Work to finalise instance state is ongoing.
     */
    pub fn should_teardown(&self) -> bool {
        matches!(
            self.state,
            InstanceState::Destroying | InstanceState::Destroyed,
        )
    }
}

#[derive(Debug, Queryable, Insertable, Identifiable)]
#[diesel(table_name = instance_event)]
#[diesel(primary_key(nodename, instance, seq))]
pub struct InstanceEvent {
    pub nodename: String,
    pub instance: InstanceSeq,
    pub seq: EventSeq,
    pub stream: String,
    pub payload: String,
    pub uploaded: bool,
    pub time: IsoDate,
}
