/*
 * Copyright 2026 Oxide Computer Company
 */

use std::collections::HashMap;
use std::iter::once;

use anyhow::{anyhow, Context as _, Result};
use aws_config::{BehaviorVersion, Region};
use aws_sdk_ec2::types::{
    AttributeBooleanValue, Filter, IpPermission, IpRange, LocationType,
    PrefixListId, ResourceType, Tag, TagSpecification,
};
use aws_sdk_ec2::Client as EC2Client;
use buildomat_client::types::{FactoryCreate, TargetCreate, TargetRedirect};
use buildomat_client::Client as Buildomat;
use buildomat_common::genkey;
use rand::seq::IteratorRandom;
use serde::Serialize;

use crate::server::ServerConfig;
use crate::with_api::with_api;
use crate::Context;

const UBUNTU_RELEASE: &str = "24.04";
const INSTANCE_TYPE: &str = "c8a.2xlarge";
const VPC_CIDR: &str = "10.0.0.0/16";

pub(crate) async fn setup(ctx: &Context) -> Result<()> {
    let root = ctx.root.join("factory-aws");
    if root.exists() {
        std::fs::remove_dir_all(&root)
            .context("failed to remove old factory-aws directory")?;
    }
    std::fs::create_dir_all(&root)
        .context("failed to create factory-aws directory")?;

    println!();
    println!("buildomat-factory-aws setup");
    println!("===========================");
    println!();

    /*
     * The user already made a choice on where to store data by selecting an AWS
     * profile and S3 bucket (which belongs to a region) when setting up the
     * core server.  Rather than asking again, reuse those choices.
     */
    let server_config = ServerConfig::from_context(ctx)?;
    let region = &server_config.storage.region;
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .profile_name(&server_config.storage.profile)
        .region(Region::new(region.clone()))
        .load()
        .await;
    let ec2 = EC2Client::new(&sdk_config);

    println!("Configuring the AWS account to run buildomat jobs...");
    let vpc = create_vpc(ctx, &ec2).await?;
    let subnet = create_subnet(ctx, &ec2, &vpc).await?;
    let sg = create_security_group(ctx, &ec2, &region, &vpc).await?;
    create_internet_gateway(ctx, &ec2, &vpc).await?;

    let ami = find_ubuntu_ami(&ec2, UBUNTU_RELEASE).await?;

    println!("Configuring the buildomat server to run jobs on AWS...");
    let (target_id, factory_token) = with_api(ctx, async |bmat| {
        let target_name = format!("ubuntu-{UBUNTU_RELEASE}");
        let target_id = create_target(bmat, target_name).await?;
        let factory_token = create_factory(bmat).await?;
        Ok((target_id, factory_token))
    })
    .await?;

    std::fs::write(
        root.join("config.toml"),
        toml::to_string_pretty(&FactoryAwsConfig {
            general: FactoryAwsGeneral {
                baseurl: server_config.general.baseurl,
            },
            factory: FactoryAwsFactory { token: factory_token },
            aws: FactoryAwsAws {
                profile: server_config.storage.profile,
                region: server_config.storage.region,
                vpc,
                subnet,
                security_group: sg,
                tag: ctx.setup_name.clone(),
                public_ip: true,
                limit_total: 50,
            },
            target: once((
                target_id,
                FactoryAwsTarget {
                    instance_type: INSTANCE_TYPE.into(),
                    root_size_gb: 200,
                    ami,
                },
            ))
            .collect(),
        })?
        .as_bytes(),
    )
    .context("failed to write factory configuration file")?;

    std::fs::write(root.join("complete"), b"true\n")
        .context("failed to mark the factory setup as complete")?;

    Ok(())
}

async fn create_vpc(ctx: &Context, ec2: &EC2Client) -> Result<String> {
    let name = &ctx.setup_name;

    /*
     * The VPC might've been created already in an incomplete setup.
     */
    let existing = ec2
        .describe_vpcs()
        .filters(filter("tag:Name", name))
        .send()
        .await
        .with_context(|| {
            format!("failed to check if a VPC named {name:?} exists")
        })?
        .vpcs()
        .first()
        .cloned();
    if let Some(existing) = existing {
        return Ok(existing.vpc_id.unwrap());
    }

    Ok(ec2
        .create_vpc()
        .cidr_block(VPC_CIDR)
        .tag_specifications(tags(ctx, ResourceType::Vpc))
        .send()
        .await
        .context("failed to create VPC")?
        .vpc
        .unwrap()
        .vpc_id
        .unwrap())
}

async fn create_subnet(
    ctx: &Context,
    ec2: &EC2Client,
    vpc: &str,
) -> Result<String> {
    let name = &ctx.setup_name;

    /*
     * The subnet might've been created already in an incomplete setup.
     */
    let existing = ec2
        .describe_subnets()
        .filters(filter("tag:Name", name))
        .filters(filter("vpc-id", vpc))
        .send()
        .await
        .with_context(|| {
            format!("failed to check if the subnet {name:?} exists")
        })?
        .subnets()
        .first()
        .cloned();
    let id = if let Some(existing) = existing {
        existing.subnet_id.unwrap()
    } else {
        let az = pick_availability_zone(ec2, INSTANCE_TYPE).await?;
        ec2.create_subnet()
            .tag_specifications(tags(ctx, ResourceType::Subnet))
            .vpc_id(vpc)
            .availability_zone(az)
            /*
             * Have this subnet cover the whole address space of the VPC, as
             * we are going to have a single subnet anyway.
             */
            .cidr_block(VPC_CIDR)
            .send()
            .await
            .context("failed to create subnet")?
            .subnet
            .unwrap()
            .subnet_id
            .unwrap()
    };

    /*
     * Configure the subnet to auto-assign IPv4 and IPv6 addresses, so that the
     * factory can choose whether to assign an IP address or not.
     */
    let booilder = AttributeBooleanValue::builder().value(true).build();
    ec2.modify_subnet_attribute()
        .subnet_id(&id)
        .map_public_ip_on_launch(booilder)
        .send()
        .await
        .with_context(|| {
            format!("failed to update settings of subnet {id:?}")
        })?;

    Ok(id)
}

async fn create_security_group(
    ctx: &Context,
    ec2: &EC2Client,
    region: &str,
    vpc: &str,
) -> Result<String> {
    let name = &ctx.setup_name;

    /*
     * While we create security groups with unique names, the group might've
     * been created already in an incomplete setup.
     */
    let existing = ec2
        .describe_security_groups()
        .filters(filter("tag:Name", name))
        .filters(filter("vpc-id", vpc))
        .send()
        .await
        .with_context(|| {
            format!("failed to check if security group {name:?} exists")
        })?
        .security_groups()
        .first()
        .cloned();

    let group_id = if let Some(existing) = existing {
        existing.group_id.unwrap()
    } else {
        ec2.create_security_group()
            .group_name(name)
            .description("VMs running buildomat jobs in a local setup.")
            .tag_specifications(tags(ctx, ResourceType::SecurityGroup))
            .vpc_id(vpc)
            .send()
            .await
            .context("failed to create AWS security group")?
            .group_id
            .unwrap()
    };

    /*
     * While on a brand new security group we can assume there are no rules,
     * existing groups might have incomplete rules.  To avoid complex logic, we
     * just delete all rules and add new ones.
     */
    let rules = ec2
        .describe_security_group_rules()
        .filters(filter("group-id", &group_id))
        .into_paginator()
        .send()
        .collect::<Result<Vec<_>, _>>()
        .await
        .with_context(|| {
            format!("failed to list security group rules for {group_id:?}")
        })?
        .into_iter()
        .flat_map(|page| page.security_group_rules().to_vec());
    for rule in rules {
        if rule.is_egress.unwrap_or(false) {
            ec2.revoke_security_group_egress()
                .group_id(&group_id)
                .security_group_rule_ids(&rule.security_group_rule_id.unwrap())
                .send()
                .await
                .context("failed to delete security group rule")?;
        } else {
            ec2.revoke_security_group_ingress()
                .group_id(&group_id)
                .security_group_rule_ids(&rule.security_group_rule_id.unwrap())
                .send()
                .await
                .context("failed to delete security group rule")?;
        }
    }

    /*
     * Authorize outbound IPv4 traffic from the instance.
     */
    ec2.authorize_security_group_egress()
        .group_id(&group_id)
        .tag_specifications(custom_tag(
            ResourceType::SecurityGroupRule,
            "outbound-traffic",
        ))
        .ip_permissions(
            IpPermission::builder()
                .ip_protocol("-1")
                .ip_ranges(IpRange::builder().cidr_ip("0.0.0.0/0").build())
                .build(),
        )
        .send()
        .await
        .context("failed to create egress rule")?;

    /*
     * Authorize EC2 Instance Connect to connect to SSH.
     */
    let instance_connect = find_managed_prefix_list(
        ec2,
        &format!("com.amazonaws.{region}.ec2-instance-connect"),
    )
    .await?;
    ec2.authorize_security_group_ingress()
        .group_id(&group_id)
        .tag_specifications(custom_tag(
            ResourceType::SecurityGroupRule,
            "ec2-instance-connect",
        ))
        .ip_permissions(
            IpPermission::builder()
                .prefix_list_ids(
                    PrefixListId::builder()
                        .prefix_list_id(instance_connect)
                        .build(),
                )
                .from_port(22)
                .to_port(22)
                .ip_protocol("tcp")
                .build(),
        )
        .send()
        .await
        .context("failed to create ingress rule for EC2 Instance Connect")?;

    Ok(group_id)
}

async fn create_internet_gateway(
    ctx: &Context,
    ec2: &EC2Client,
    vpc: &str,
) -> Result<()> {
    let name = &ctx.setup_name;

    /*
     * The gateway might've been created already in an incomplete setup.
     */
    let existing = ec2
        .describe_internet_gateways()
        .filters(filter("tag:Name", name))
        .send()
        .await
        .with_context(|| {
            format!("failed to check if internet gateway {name:?} exists")
        })?
        .internet_gateways()
        .first()
        .cloned();

    let gateway = if let Some(existing) = existing {
        existing
    } else {
        ec2.create_internet_gateway()
            .tag_specifications(tags(ctx, ResourceType::InternetGateway))
            .send()
            .await
            .context("failed to create internet gateway")?
            .internet_gateway
            .unwrap()
    };
    let gateway_id = gateway.internet_gateway_id.clone().unwrap();

    /*
     * When created, gateways are not attached to any VPC: attach it.
     */
    if gateway.attachments().is_empty() {
        ec2.attach_internet_gateway()
            .vpc_id(vpc)
            .internet_gateway_id(&gateway_id)
            .send()
            .await
            .with_context(|| {
                format!("failed to attach IG {gateway_id:?} to VPC {vpc:?}")
            })?;
    }

    /*
     * Attach the internet gateway to the VPC.
     */
    let route_tables = ec2
        .describe_route_tables()
        .filters(filter("vpc-id", vpc))
        .into_paginator()
        .send()
        .collect::<Result<Vec<_>, _>>()
        .await
        .with_context(|| {
            format!("failed to list route tables for VPC {vpc:?}")
        })?
        .into_iter()
        .flat_map(|page| page.route_tables().to_vec());
    for route_table in route_tables {
        let id = route_table.route_table_id().unwrap();
        if route_table
            .routes()
            .iter()
            .all(|route| route.destination_cidr_block() != Some("0.0.0.0/0"))
        {
            ec2.create_route()
                .route_table_id(id)
                .gateway_id(&gateway_id)
                .destination_cidr_block("0.0.0.0/0")
                .send()
                .await
                .with_context(|| {
                    format!(
                        "failed to add gateway {gateway_id:?} \
                         to route table {id:?}"
                    )
                })?;
        }
    }

    Ok(())
}

/*
 * We are configuring the factory with an Ubuntu AMI by default as the user
 * might not have access to an AWS account with Helios images.
 *
 * https://documentation.ubuntu.com/aws/aws-how-to/instances/find-ubuntu-images/
 */
async fn find_ubuntu_ami(ec2: &EC2Client, release: &str) -> Result<String> {
    const CANONICAL_ACCOUNT: &str = "099720109477";

    let mut images = ec2
        .describe_images()
        .filters(filter(
            "name",
            format!("ubuntu/images/hvm-ssd*/ubuntu-*-{release}-amd64-server-*"),
        ))
        .owners(CANONICAL_ACCOUNT)
        .into_paginator()
        .send()
        .collect::<Result<Vec<_>, _>>()
        .await
        .context("failed to list Ubuntu images in AWS")?
        .into_iter()
        .flat_map(|page| page.images().to_vec())
        .collect::<Vec<_>>();

    images.sort_by_key(|image| image.creation_date.clone());
    let image = images
        .pop()
        .ok_or_else(|| anyhow!("couldn't find any Ubuntu {release} images"))?;
    assert_eq!(image.owner_id(), Some(CANONICAL_ACCOUNT));

    Ok(image.image_id.unwrap())
}

/**
 * AWS maintains list of IPs used by some of their services.  We can use them to
 * allow those services to connect to the security group, without allowing any
 * other service or external user.
 */
async fn find_managed_prefix_list(
    ec2: &EC2Client,
    name: &str,
) -> Result<String> {
    ec2.describe_managed_prefix_lists()
        .filters(filter("prefix-list-name", name))
        .filters(filter("owner-id", "AWS"))
        .send()
        .await
        .context("failed to retrieve managed prefix lists")?
        .prefix_lists()
        .first()
        .and_then(|pl| pl.prefix_list_id.clone())
        .ok_or_else(|| anyhow!("couldn't find managed prefix list {name}"))
}

/**
 * AWS is annoying and not all availability zones in a region support any given
 * instance type.  For example, at the time of writing this, only two AZs in
 * "eu-central-1" (out of three) support the "c8a.2xlarge" instance type. We
 * need to learn which AZs support the instance we care about, and be careful
 * later to only choose an AZ supporting it.
 */
async fn pick_availability_zone(
    ec2: &EC2Client,
    type_: &str,
) -> Result<String> {
    ec2.describe_instance_type_offerings()
        .location_type(LocationType::AvailabilityZone)
        .filters(Filter::builder().name("instance-type").values(type_).build())
        .into_paginator()
        .send()
        .collect::<Result<Vec<_>, _>>()
        .await
        .with_context(|| {
            format!("failed to get the AZs supporting instance {type_:?}")
        })?
        .into_iter()
        .flat_map(|page| page.instance_type_offerings().to_vec())
        .map(|ito| ito.location.unwrap())
        .choose(&mut rand::rng())
        .ok_or_else(|| anyhow!("no AZs found supporting instance {type_}"))
}

async fn create_factory(bmat: &Buildomat) -> Result<String> {
    /*
     * Buildomat doesn't have a way to retrieve or regenerate the token of an
     * existing factory.  Since the setup process might fail between here and
     * completion, we can't use a fixed name, since executing the setup again
     * would try to create a factory with a duplicate name.
     */
    let name = format!("aws-{}", genkey(8));

    Ok(bmat
        .factory_create()
        .body(FactoryCreate { name })
        .send()
        .await
        .context("failed to create the buildomat factory")?
        .into_inner()
        .token)
}

async fn create_target(bmat: &Buildomat, name: String) -> Result<String> {
    /*
     * Create the requested target if missing.
     */
    let existing = bmat
        .targets_list()
        .send()
        .await
        .context("failed to list buildomat targets")?
        .into_inner();
    let id = if let Some(local) = existing.iter().find(|t| t.name == name) {
        local.id.clone()
    } else {
        let local = bmat
            .target_create()
            .body(TargetCreate {
                name: name.clone(),
                desc: "Target created by \"cargo xtask local setup\".".into(),
            })
            .send()
            .await
            .context("failed to create the local buildomat target")?
            .into_inner();
        local.id
    };

    /*
     * Point "default" to the target we just created.
     */
    if let Some(default) = existing.iter().find(|t| t.name == "default") {
        bmat.target_redirect()
            .target(&default.id)
            .body(TargetRedirect { redirect: Some(id.clone()) })
            .send()
            .await
            .with_context(|| {
                format!("failed to redirect target \"default\" to {name:?}")
            })?;
    }

    Ok(id)
}

fn filter(name: &str, value: impl Into<String>) -> Filter {
    Filter::builder().name(name).values(value.into()).build()
}

fn tags(ctx: &Context, resource_type: ResourceType) -> TagSpecification {
    custom_tag(resource_type, &ctx.setup_name)
}

fn custom_tag(resource_type: ResourceType, name: &str) -> TagSpecification {
    TagSpecification::builder()
        .resource_type(resource_type)
        .tags(Tag::builder().key("Name").value(name).build())
        .build()
}

#[derive(Serialize)]
struct FactoryAwsConfig {
    general: FactoryAwsGeneral,
    factory: FactoryAwsFactory,
    aws: FactoryAwsAws,
    target: HashMap<String, FactoryAwsTarget>,
}

#[derive(Serialize)]
struct FactoryAwsGeneral {
    baseurl: String,
}

#[derive(Serialize)]
struct FactoryAwsFactory {
    token: String,
}

#[derive(Serialize)]
struct FactoryAwsAws {
    profile: String,
    region: String,
    vpc: String,
    subnet: String,
    security_group: String,
    tag: String,
    limit_total: u32,
    public_ip: bool,
}

#[derive(Serialize)]
struct FactoryAwsTarget {
    instance_type: String,
    root_size_gb: u32,
    ami: String,
}
