/*
 * Copyright 2026 Oxide Computer Company
 */

use crate::bootstrap_agent::BootstrapContext;
use crate::db::types::WorkerId;
use anyhow::{anyhow, Result};
use buildomat_common::unix::{Gid, Uid};
use slog::{info, Logger};
use smf::scf_type_t::{SCF_TYPE_ASTRING, SCF_TYPE_BOOLEAN};
use smf::{CommitResult, Instance, Scf, State, Transaction};

const SMF_SERVICE: &str = "site/buildomat/factory-user-worker";

pub(super) fn start(
    id: WorkerId,
    uid: Uid,
    gid: Gid,
    ctx: &BootstrapContext,
) -> Result<()> {
    let scf = Scf::new()?;
    let scope = scf.scope_local()?;
    let service = scope
        .get_service(SMF_SERVICE)?
        .ok_or_else(|| anyhow!("missing SMF service {SMF_SERVICE:?}"))?;

    let instance = match service.get_instance(&instance_name(id))? {
        Some(instance) => instance,
        None => service.add_instance(&instance_name(id))?,
    };

    /*
     * Unfortunately the SMF crate doesn't have a method to get a
     * property group by name, so we have to search all of them.
     */
    let pg_svc_start = service
        .pgs()?
        .flat_map(|result| result.ok())
        .find(|pg| pg.name().ok().as_deref() == Some("start"))
        .ok_or_else(|| anyhow!("missing \"start\" service property group"))?;

    edit_property_group(&instance, "start", "framework", |tx| {
        /*
         * When the instance is created there is no "start" property group at
         * the instance level, only at the service level.
         *
         * Unfortunately, creating the group in the instance doesn't inherit
         * properties from the service, so we have to do that ourselves.
         * Otherwise, properties defined at the service level would disappear.
         */
        for property in pg_svc_start.properties()? {
            let property = property?;
            if let Some(value) = property.value()? {
                tx.property_ensure(
                    &property.name()?,
                    property.type_()?,
                    &value.as_string()?,
                )?;
            };
        }

        /*
         * Set the user and group the SMF instance will run as.
         */
        tx.property_ensure("use_profile", SCF_TYPE_BOOLEAN, "false")?;
        tx.property_ensure("user", SCF_TYPE_ASTRING, &uid.0.to_string())?;
        tx.property_ensure("group", SCF_TYPE_ASTRING, &gid.0.to_string())?;

        Ok(())
    })?;

    edit_property_group(&instance, "buildomat", "application", |tx| {
        let BootstrapContext { baseurl, bootstrap_token, chroot, env: _ } = ctx;

        tx.property_ensure("baseurl", SCF_TYPE_ASTRING, baseurl)?;
        tx.property_ensure(
            "bootstrap_token",
            SCF_TYPE_ASTRING,
            bootstrap_token,
        )?;
        tx.property_ensure(
            "chroot",
            SCF_TYPE_ASTRING,
            chroot
                .to_str()
                .ok_or_else(|| anyhow!("non-UTF-8 path: {chroot:?}"))?,
        )?;
        Ok(())
    })?;

    /*
     * To avoid losing the key/value structure of environment variables, create
     * a separate property group for them.
     */
    edit_property_group(&instance, "buildomat-env", "application", |tx| {
        for (key, value) in &ctx.env {
            tx.property_ensure(key, SCF_TYPE_ASTRING, value)?;
        }
        Ok(())
    })?;

    /*
     * When creating an instance of a service, it's created in the SMF
     * repository database (svc.configd) but it doesn't seem to end up fully
     * initialised by the restarter daemon (svc.startd) unless the restarter is
     * told to do something.  That causes weird errors when attempting to look
     * at the state of the service.
     *
     * The presence of a running snapshot indicates the restarter daemon knows
     * about the service, so if none is present we do any action to let the
     * restarter the service actually exists.
     */
    if instance.get_running_snapshot().is_err() {
        instance.disable(false)?;
    }

    instance.enable(false)?;

    Ok(())
}

pub(super) fn state(id: WorkerId) -> Result<InstState> {
    let scf = Scf::new()?;
    let scope = scf.scope_local()?;
    let service = scope
        .get_service(SMF_SERVICE)?
        .ok_or_else(|| anyhow!("missing SMF service {SMF_SERVICE:?}"))?;

    let Some(instance) = service.get_instance(&instance_name(id))? else {
        return Ok(InstState::Missing);
    };

    let (current, target) = instance.states()?;
    Ok(InstState::Present { current, target })
}

pub(super) fn destroy(log: &Logger, id: WorkerId) -> Result<Destroy> {
    let scf = Scf::new()?;
    let scope = scf.scope_local()?;
    let service = scope
        .get_service(SMF_SERVICE)?
        .ok_or_else(|| anyhow!("missing SMF service {SMF_SERVICE:?}"))?;

    let Some(instance) = service.get_instance(&instance_name(id))? else {
        return Ok(Destroy::Destroyed);
    };

    let (current, target) = instance.states()?;
    if target.is_some() {
        return Ok(Destroy::Destroying);
    }
    match current {
        Some(State::Disabled | State::Maintenance) => {
            /*
             * This is a terminal state and we can delete the
             * service instance.
             */
            instance.delete()?;
            Ok(Destroy::Destroyed)
        }
        _ => {
            info!(log, "disabling SMF instance for worker {id}");
            instance.disable(true)?;
            Ok(Destroy::Destroying)
        }
    }
}

fn edit_property_group(
    instance: &Instance<'_>,
    name: &str,
    pgtype: &str,
    edit: impl Fn(&Transaction<'_>) -> Result<()>,
) -> Result<()> {
    let pg = match instance.get_pg(name)? {
        Some(pg) => pg,
        None => instance.add_pg(name, pgtype)?,
    };
    loop {
        let tx = pg.transaction()?;
        tx.start()?;
        edit(&tx)?;
        match tx.commit()? {
            CommitResult::Success => break,
            CommitResult::OutOfDate => pg.update()?,
        }
    }
    Ok(())
}

fn instance_name(id: WorkerId) -> String {
    format!("bmat-{id}")
}

pub(super) enum InstState {
    Missing,
    Present { current: Option<State>, target: Option<State> },
}

pub(super) enum Destroy {
    Destroying,
    Destroyed,
}
