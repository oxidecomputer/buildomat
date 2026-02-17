use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::{BehaviorVersion, ConfigLoader, Region, SdkConfig};
use aws_credential_types::Credentials;
use aws_credential_types::provider::SharedCredentialsProvider;

pub struct AwsConfig {
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub profile: Option<String>,
    pub region: String,
}

impl AwsConfig {
    pub async fn into_sdk_config(self) -> Result<SdkConfig, AwsConfigError> {
        let creds: SharedCredentialsProvider =
            match (self.access_key_id, self.secret_access_key, self.profile) {
                /*
                 * When an hardcoded AWS access key is present, provide it statically.
                 */
                (Some(aki), Some(sak), None) => SharedCredentialsProvider::new(
                    Credentials::new(aki, sak, None, None, "buildomat"),
                ),
                /*
                 * When a profile is selected, tell the default credential chain
                 * to use it, dynamically fetching the access key.  This could
                 * be used authenticate with AWS SSO on a developer machine.
                 */
                (None, None, Some(profile)) => SharedCredentialsProvider::new(
                    DefaultCredentialsChain::builder()
                        .profile_name(&profile)
                        .build()
                        .await,
                ),
                /*
                 * When nothing is selected, use the default credential chain to
                 * dynamically fetch the access key.  This could be used to
                 * authenticate an AWS instance using their metadata service.
                 */
                (None, None, None) => SharedCredentialsProvider::new(
                    DefaultCredentialsChain::builder().build().await,
                ),
                /*
                 * Provide good error messages for invalid configurations.
                 */
                (Some(_), None, _) | (None, Some(_), _) => {
                    return Err(AwsConfigError::IncompleteAccessKey);
                }
                (Some(_), Some(_), Some(_)) => {
                    return Err(AwsConfigError::MultipleCredentials);
                }
            };

        Ok(ConfigLoader::default()
            .region(Region::new(self.region))
            .credentials_provider(creds)
            .behavior_version(BehaviorVersion::v2026_01_12())
            .load()
            .await)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AwsConfigError {
    #[error("both \"access_key_id\" and \"secret_access_key\" are required")]
    IncompleteAccessKey,
    #[error("cannot use both an AWS profile and hardcoded credentials")]
    MultipleCredentials,
}
