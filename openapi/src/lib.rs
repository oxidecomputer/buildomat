mod progenitor_client;

pub use progenitor_client::{ByteStream, Error, ResponseValue};
pub mod types {
    use serde::{Deserialize, Serialize};
    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct DependSubmit {
        pub copy_outputs: bool,
        pub on_completed: bool,
        pub on_failed: bool,
        pub prior_job: String,
    }

    #[doc = "Error information from a response."]
    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct Error {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub error_code: Option<String>,
        pub message: String,
        pub request_id: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct FactoryCreate {
        pub name: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct FactoryCreateResult {
        pub id: String,
        pub name: String,
        pub token: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct FactoryLease {
        pub job: String,
        pub target: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct FactoryLeaseResult {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub lease: Option<FactoryLease>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct FactoryPingResult {
        pub ok: bool,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct FactoryWhatsNext {
        pub supported_targets: Vec<String>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct FactoryWorker {
        pub bootstrap: String,
        pub id: String,
        pub online: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub private: Option<String>,
        pub recycle: bool,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct FactoryWorkerAppend {
        pub payload: String,
        pub stream: String,
        pub time: chrono::DateTime<chrono::offset::Utc>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct FactoryWorkerAppendResult {
        pub retry: bool,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct FactoryWorkerAssociate {
        pub private: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct FactoryWorkerCreate {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub job: Option<String>,
        pub target: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub wait_for_flush: Option<bool>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct FactoryWorkerResult {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub worker: Option<FactoryWorker>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct Job {
        pub cancelled: bool,
        pub id: String,
        pub name: String,
        pub output_rules: Vec<String>,
        pub owner: String,
        pub state: String,
        pub tags: std::collections::HashMap<String, String>,
        pub target: String,
        pub target_real: String,
        pub tasks: Vec<Task>,
        #[serde(
            default,
            skip_serializing_if = "std::collections::HashMap::is_empty"
        )]
        pub times: std::collections::HashMap<
            String,
            chrono::DateTime<chrono::offset::Utc>,
        >,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct JobAddInput {
        pub chunks: Vec<String>,
        pub name: String,
        pub size: i64,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct JobEvent {
        pub payload: String,
        pub seq: u32,
        pub stream: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub task: Option<u32>,
        pub time: chrono::DateTime<chrono::offset::Utc>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub time_remote: Option<chrono::DateTime<chrono::offset::Utc>>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct JobOutput {
        pub id: String,
        pub path: String,
        pub size: u64,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct JobOutputPublish {
        pub name: String,
        pub series: String,
        pub version: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct JobSubmit {
        #[serde(
            default,
            skip_serializing_if = "std::collections::HashMap::is_empty"
        )]
        pub depends: std::collections::HashMap<String, DependSubmit>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        pub inputs: Vec<String>,
        pub name: String,
        pub output_rules: Vec<String>,
        #[serde(
            default,
            skip_serializing_if = "std::collections::HashMap::is_empty"
        )]
        pub tags: std::collections::HashMap<String, String>,
        pub target: String,
        pub tasks: Vec<TaskSubmit>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct JobSubmitResult {
        pub id: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct Target {
        pub desc: String,
        pub id: String,
        pub name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub privilege: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub redirect: Option<String>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct TargetCreate {
        pub desc: String,
        pub name: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct TargetCreateResult {
        pub id: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct TargetRedirect {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub redirect: Option<String>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct TargetRename {
        pub new_name: String,
        pub signpost_description: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct Task {
        pub env: std::collections::HashMap<String, String>,
        pub env_clear: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub gid: Option<u32>,
        pub name: String,
        pub script: String,
        pub state: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub uid: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub workdir: Option<String>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct TaskSubmit {
        pub env: std::collections::HashMap<String, String>,
        pub env_clear: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub gid: Option<u32>,
        pub name: String,
        pub script: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub uid: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub workdir: Option<String>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct UploadedChunk {
        pub id: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct User {
        pub id: String,
        pub name: String,
        pub privileges: Vec<String>,
        pub time_create: chrono::DateTime<chrono::offset::Utc>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct UserCreate {
        pub name: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct UserCreateResult {
        pub id: String,
        pub name: String,
        pub token: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct WhoamiResult {
        pub id: String,
        pub name: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct Worker {
        pub bootstrap: bool,
        pub deleted: bool,
        pub factory: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub factory_private: Option<String>,
        pub id: String,
        pub jobs: Vec<WorkerJob>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub lastping: Option<chrono::DateTime<chrono::offset::Utc>>,
        pub recycle: bool,
        pub target: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct WorkerAddOutput {
        pub chunks: Vec<String>,
        pub path: String,
        pub size: i64,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct WorkerAppendJob {
        pub payload: String,
        pub stream: String,
        pub time: chrono::DateTime<chrono::offset::Utc>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct WorkerBootstrap {
        pub bootstrap: String,
        pub token: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct WorkerBootstrapResult {
        pub id: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct WorkerCompleteJob {
        pub failed: bool,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct WorkerCompleteTask {
        pub failed: bool,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct WorkerJob {
        pub id: String,
        pub name: String,
        pub owner: String,
        pub state: String,
        pub tags: std::collections::HashMap<String, String>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct WorkerPingInput {
        pub id: String,
        pub name: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct WorkerPingJob {
        pub id: String,
        pub inputs: Vec<WorkerPingInput>,
        pub name: String,
        pub output_rules: Vec<String>,
        pub tasks: Vec<WorkerPingTask>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct WorkerPingResult {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub job: Option<WorkerPingJob>,
        pub poweroff: bool,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct WorkerPingTask {
        pub env: std::collections::HashMap<String, String>,
        pub env_clear: bool,
        pub gid: u32,
        pub id: u32,
        pub name: String,
        pub script: String,
        pub uid: u32,
        pub workdir: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct WorkersResult {
        pub workers: Vec<Worker>,
    }
}

#[derive(Clone)]
pub struct Client {
    baseurl: String,
    client: reqwest::Client,
}

impl Client {
    pub fn new(baseurl: &str) -> Self {
        let dur = std::time::Duration::from_secs(15);
        let client = reqwest::ClientBuilder::new()
            .connect_timeout(dur)
            .timeout(dur)
            .build()
            .unwrap();
        Self::new_with_client(baseurl, client)
    }

    pub fn new_with_client(baseurl: &str, client: reqwest::Client) -> Self {
        Self { baseurl: baseurl.to_string(), client }
    }

    pub fn baseurl(&self) -> &String {
        &self.baseurl
    }

    pub fn client(&self) -> &reqwest::Client {
        &self.client
    }

    #[doc = "Sends a `POST` request to `/0/admin/factory`"]
    pub async fn factory_create<'a>(
        &'a self,
        body: &'a types::FactoryCreate,
    ) -> Result<ResponseValue<types::FactoryCreateResult>, Error<types::Error>>
    {
        let url = format!("{}/0/admin/factory", self.baseurl,);
        let request = self.client.post(url).json(body).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            201u16 => ResponseValue::from_response(response).await,
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `GET` request to `/0/admin/jobs`"]
    pub async fn admin_jobs_get<'a>(
        &'a self,
        active: Option<bool>,
        completed: Option<u64>,
    ) -> Result<ResponseValue<Vec<types::Job>>, Error<types::Error>> {
        let url = format!("{}/0/admin/jobs", self.baseurl,);
        let mut query = Vec::new();
        if let Some(v) = &active {
            query.push(("active", v.to_string()));
        }

        if let Some(v) = &completed {
            query.push(("completed", v.to_string()));
        }

        let request = self.client.get(url).query(&query).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `GET` request to `/0/admin/jobs/{job}`"]
    pub async fn admin_job_get<'a>(
        &'a self,
        job: &'a str,
    ) -> Result<ResponseValue<types::Job>, Error<types::Error>> {
        let url = format!(
            "{}/0/admin/jobs/{}",
            self.baseurl,
            progenitor_client::encode_path(&job.to_string()),
        );
        let request = self.client.get(url).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `POST` request to `/0/admin/target`"]
    pub async fn target_create<'a>(
        &'a self,
        body: &'a types::TargetCreate,
    ) -> Result<ResponseValue<types::TargetCreateResult>, Error<types::Error>>
    {
        let url = format!("{}/0/admin/target", self.baseurl,);
        let request = self.client.post(url).json(body).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            201u16 => ResponseValue::from_response(response).await,
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `GET` request to `/0/admin/targets`"]
    pub async fn targets_list<'a>(
        &'a self,
    ) -> Result<ResponseValue<Vec<types::Target>>, Error<types::Error>> {
        let url = format!("{}/0/admin/targets", self.baseurl,);
        let request = self.client.get(url).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `PUT` request to `/0/admin/targets/{target}/redirect`"]
    pub async fn target_redirect<'a>(
        &'a self,
        target: &'a str,
        body: &'a types::TargetRedirect,
    ) -> Result<ResponseValue<()>, Error<types::Error>> {
        let url = format!(
            "{}/0/admin/targets/{}/redirect",
            self.baseurl,
            progenitor_client::encode_path(&target.to_string()),
        );
        let request = self.client.put(url).json(body).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            204u16 => Ok(ResponseValue::empty(response)),
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `POST` request to `/0/admin/targets/{target}/rename`"]
    pub async fn target_rename<'a>(
        &'a self,
        target: &'a str,
        body: &'a types::TargetRename,
    ) -> Result<ResponseValue<types::TargetCreateResult>, Error<types::Error>>
    {
        let url = format!(
            "{}/0/admin/targets/{}/rename",
            self.baseurl,
            progenitor_client::encode_path(&target.to_string()),
        );
        let request = self.client.post(url).json(body).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            201u16 => ResponseValue::from_response(response).await,
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `DELETE` request to `/0/admin/targets/{target}/require`"]
    pub async fn target_require_no_privilege<'a>(
        &'a self,
        target: &'a str,
    ) -> Result<ResponseValue<()>, Error<types::Error>> {
        let url = format!(
            "{}/0/admin/targets/{}/require",
            self.baseurl,
            progenitor_client::encode_path(&target.to_string()),
        );
        let request = self.client.delete(url).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            204u16 => Ok(ResponseValue::empty(response)),
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `PUT` request to `/0/admin/targets/{target}/require/{privilege}`"]
    pub async fn target_require_privilege<'a>(
        &'a self,
        target: &'a str,
        privilege: &'a str,
    ) -> Result<ResponseValue<()>, Error<types::Error>> {
        let url = format!(
            "{}/0/admin/targets/{}/require/{}",
            self.baseurl,
            progenitor_client::encode_path(&target.to_string()),
            progenitor_client::encode_path(&privilege.to_string()),
        );
        let request = self.client.put(url).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            204u16 => Ok(ResponseValue::empty(response)),
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `POST` request to `/0/control/hold`"]
    pub async fn control_hold<'a>(
        &'a self,
    ) -> Result<ResponseValue<()>, Error<types::Error>> {
        let url = format!("{}/0/control/hold", self.baseurl,);
        let request = self.client.post(url).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            204u16 => Ok(ResponseValue::empty(response)),
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `POST` request to `/0/control/resume`"]
    pub async fn control_resume<'a>(
        &'a self,
    ) -> Result<ResponseValue<()>, Error<types::Error>> {
        let url = format!("{}/0/control/resume", self.baseurl,);
        let request = self.client.post(url).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            204u16 => Ok(ResponseValue::empty(response)),
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `POST` request to `/0/factory/lease`"]
    pub async fn factory_lease<'a>(
        &'a self,
        body: &'a types::FactoryWhatsNext,
    ) -> Result<ResponseValue<types::FactoryLeaseResult>, Error<types::Error>>
    {
        let url = format!("{}/0/factory/lease", self.baseurl,);
        let request = self.client.post(url).json(body).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `POST` request to `/0/factory/lease/{job}`"]
    pub async fn factory_lease_renew<'a>(
        &'a self,
        job: &'a str,
    ) -> Result<ResponseValue<bool>, Error<types::Error>> {
        let url = format!(
            "{}/0/factory/lease/{}",
            self.baseurl,
            progenitor_client::encode_path(&job.to_string()),
        );
        let request = self.client.post(url).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `GET` request to `/0/factory/ping`"]
    pub async fn factory_ping<'a>(
        &'a self,
    ) -> Result<ResponseValue<types::FactoryPingResult>, Error<types::Error>>
    {
        let url = format!("{}/0/factory/ping", self.baseurl,);
        let request = self.client.get(url).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `POST` request to `/0/factory/worker`"]
    pub async fn factory_worker_create<'a>(
        &'a self,
        body: &'a types::FactoryWorkerCreate,
    ) -> Result<ResponseValue<types::FactoryWorker>, Error<types::Error>> {
        let url = format!("{}/0/factory/worker", self.baseurl,);
        let request = self.client.post(url).json(body).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            201u16 => ResponseValue::from_response(response).await,
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `GET` request to `/0/factory/worker/{worker}`"]
    pub async fn factory_worker_get<'a>(
        &'a self,
        worker: &'a str,
    ) -> Result<ResponseValue<types::FactoryWorkerResult>, Error<types::Error>>
    {
        let url = format!(
            "{}/0/factory/worker/{}",
            self.baseurl,
            progenitor_client::encode_path(&worker.to_string()),
        );
        let request = self.client.get(url).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `DELETE` request to `/0/factory/worker/{worker}`"]
    pub async fn factory_worker_destroy<'a>(
        &'a self,
        worker: &'a str,
    ) -> Result<ResponseValue<bool>, Error<types::Error>> {
        let url = format!(
            "{}/0/factory/worker/{}",
            self.baseurl,
            progenitor_client::encode_path(&worker.to_string()),
        );
        let request = self.client.delete(url).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `PATCH` request to `/0/factory/worker/{worker}`"]
    pub async fn factory_worker_associate<'a>(
        &'a self,
        worker: &'a str,
        body: &'a types::FactoryWorkerAssociate,
    ) -> Result<ResponseValue<()>, Error<types::Error>> {
        let url = format!(
            "{}/0/factory/worker/{}",
            self.baseurl,
            progenitor_client::encode_path(&worker.to_string()),
        );
        let request = self.client.patch(url).json(body).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            204u16 => Ok(ResponseValue::empty(response)),
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `POST` request to `/0/factory/worker/{worker}/append`"]
    pub async fn factory_worker_append<'a>(
        &'a self,
        worker: &'a str,
        body: &'a types::FactoryWorkerAppend,
    ) -> Result<
        ResponseValue<types::FactoryWorkerAppendResult>,
        Error<types::Error>,
    > {
        let url = format!(
            "{}/0/factory/worker/{}/append",
            self.baseurl,
            progenitor_client::encode_path(&worker.to_string()),
        );
        let request = self.client.post(url).json(body).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `POST` request to `/0/factory/worker/{worker}/flush`"]
    pub async fn factory_worker_flush<'a>(
        &'a self,
        worker: &'a str,
    ) -> Result<ResponseValue<()>, Error<types::Error>> {
        let url = format!(
            "{}/0/factory/worker/{}/flush",
            self.baseurl,
            progenitor_client::encode_path(&worker.to_string()),
        );
        let request = self.client.post(url).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            204u16 => Ok(ResponseValue::empty(response)),
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `GET` request to `/0/factory/workers`"]
    pub async fn factory_workers<'a>(
        &'a self,
    ) -> Result<ResponseValue<Vec<types::FactoryWorker>>, Error<types::Error>>
    {
        let url = format!("{}/0/factory/workers", self.baseurl,);
        let request = self.client.get(url).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `GET` request to `/0/job/{job}`"]
    pub async fn job_get<'a>(
        &'a self,
        job: &'a str,
    ) -> Result<ResponseValue<types::Job>, Error<types::Error>> {
        let url = format!(
            "{}/0/job/{}",
            self.baseurl,
            progenitor_client::encode_path(&job.to_string()),
        );
        let request = self.client.get(url).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `GET` request to `/0/jobs`"]
    pub async fn jobs_get<'a>(
        &'a self,
    ) -> Result<ResponseValue<Vec<types::Job>>, Error<types::Error>> {
        let url = format!("{}/0/jobs", self.baseurl,);
        let request = self.client.get(url).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `POST` request to `/0/jobs`"]
    pub async fn job_submit<'a>(
        &'a self,
        body: &'a types::JobSubmit,
    ) -> Result<ResponseValue<types::JobSubmitResult>, Error<types::Error>>
    {
        let url = format!("{}/0/jobs", self.baseurl,);
        let request = self.client.post(url).json(body).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            201u16 => ResponseValue::from_response(response).await,
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `POST` request to `/0/jobs/{job}/cancel`"]
    pub async fn job_cancel<'a>(
        &'a self,
        job: &'a str,
    ) -> Result<ResponseValue<()>, Error<types::Error>> {
        let url = format!(
            "{}/0/jobs/{}/cancel",
            self.baseurl,
            progenitor_client::encode_path(&job.to_string()),
        );
        let request = self.client.post(url).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            204u16 => Ok(ResponseValue::empty(response)),
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `POST` request to `/0/jobs/{job}/chunk`"]
    pub async fn job_upload_chunk<'a, B: Into<reqwest::Body>>(
        &'a self,
        job: &'a str,
        body: B,
    ) -> Result<ResponseValue<types::UploadedChunk>, Error<types::Error>> {
        let url = format!(
            "{}/0/jobs/{}/chunk",
            self.baseurl,
            progenitor_client::encode_path(&job.to_string()),
        );
        let request = self.client.post(url).body(body).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            201u16 => ResponseValue::from_response(response).await,
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `GET` request to `/0/jobs/{job}/events`"]
    pub async fn job_events_get<'a>(
        &'a self,
        job: &'a str,
        minseq: Option<u32>,
    ) -> Result<ResponseValue<Vec<types::JobEvent>>, Error<types::Error>> {
        let url = format!(
            "{}/0/jobs/{}/events",
            self.baseurl,
            progenitor_client::encode_path(&job.to_string()),
        );
        let mut query = Vec::new();
        if let Some(v) = &minseq {
            query.push(("minseq", v.to_string()));
        }

        let request = self.client.get(url).query(&query).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `POST` request to `/0/jobs/{job}/input`"]
    pub async fn job_add_input<'a>(
        &'a self,
        job: &'a str,
        body: &'a types::JobAddInput,
    ) -> Result<ResponseValue<()>, Error<types::Error>> {
        let url = format!(
            "{}/0/jobs/{}/input",
            self.baseurl,
            progenitor_client::encode_path(&job.to_string()),
        );
        let request = self.client.post(url).json(body).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            204u16 => Ok(ResponseValue::empty(response)),
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `GET` request to `/0/jobs/{job}/outputs`"]
    pub async fn job_outputs_get<'a>(
        &'a self,
        job: &'a str,
    ) -> Result<ResponseValue<Vec<types::JobOutput>>, Error<types::Error>> {
        let url = format!(
            "{}/0/jobs/{}/outputs",
            self.baseurl,
            progenitor_client::encode_path(&job.to_string()),
        );
        let request = self.client.get(url).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `GET` request to `/0/jobs/{job}/outputs/{output}`"]
    pub async fn job_output_download<'a>(
        &'a self,
        job: &'a str,
        output: &'a str,
    ) -> Result<ResponseValue<ByteStream>, Error<ByteStream>> {
        let url = format!(
            "{}/0/jobs/{}/outputs/{}",
            self.baseurl,
            progenitor_client::encode_path(&job.to_string()),
            progenitor_client::encode_path(&output.to_string()),
        );
        let request = self.client.get(url).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            200..=299 => Ok(ResponseValue::stream(response)),
            _ => Err(Error::ErrorResponse(ResponseValue::stream(response))),
        }
    }

    #[doc = "Sends a `POST` request to `/0/jobs/{job}/outputs/{output}/publish`"]
    pub async fn job_output_publish<'a>(
        &'a self,
        job: &'a str,
        output: &'a str,
        body: &'a types::JobOutputPublish,
    ) -> Result<ResponseValue<()>, Error<types::Error>> {
        let url = format!(
            "{}/0/jobs/{}/outputs/{}/publish",
            self.baseurl,
            progenitor_client::encode_path(&job.to_string()),
            progenitor_client::encode_path(&output.to_string()),
        );
        let request = self.client.post(url).json(body).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            204u16 => Ok(ResponseValue::empty(response)),
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `GET` request to `/0/public/file/{username}/{series}/{version}/{name}`"]
    pub async fn public_file_download<'a>(
        &'a self,
        username: &'a str,
        series: &'a str,
        version: &'a str,
        name: &'a str,
    ) -> Result<ResponseValue<ByteStream>, Error<ByteStream>> {
        let url = format!(
            "{}/0/public/file/{}/{}/{}/{}",
            self.baseurl,
            progenitor_client::encode_path(&username.to_string()),
            progenitor_client::encode_path(&series.to_string()),
            progenitor_client::encode_path(&version.to_string()),
            progenitor_client::encode_path(&name.to_string()),
        );
        let request = self.client.get(url).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            200..=299 => Ok(ResponseValue::stream(response)),
            _ => Err(Error::ErrorResponse(ResponseValue::stream(response))),
        }
    }

    #[doc = "Sends a `GET` request to `/0/users`"]
    pub async fn users_list<'a>(
        &'a self,
    ) -> Result<ResponseValue<Vec<types::User>>, Error<types::Error>> {
        let url = format!("{}/0/users", self.baseurl,);
        let request = self.client.get(url).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `POST` request to `/0/users`"]
    pub async fn user_create<'a>(
        &'a self,
        body: &'a types::UserCreate,
    ) -> Result<ResponseValue<types::UserCreateResult>, Error<types::Error>>
    {
        let url = format!("{}/0/users", self.baseurl,);
        let request = self.client.post(url).json(body).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            201u16 => ResponseValue::from_response(response).await,
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `GET` request to `/0/users/{user}`"]
    pub async fn user_get<'a>(
        &'a self,
        user: &'a str,
    ) -> Result<ResponseValue<types::User>, Error<types::Error>> {
        let url = format!(
            "{}/0/users/{}",
            self.baseurl,
            progenitor_client::encode_path(&user.to_string()),
        );
        let request = self.client.get(url).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `PUT` request to `/0/users/{user}/privilege/{privilege}`"]
    pub async fn user_privilege_grant<'a>(
        &'a self,
        user: &'a str,
        privilege: &'a str,
    ) -> Result<ResponseValue<()>, Error<types::Error>> {
        let url = format!(
            "{}/0/users/{}/privilege/{}",
            self.baseurl,
            progenitor_client::encode_path(&user.to_string()),
            progenitor_client::encode_path(&privilege.to_string()),
        );
        let request = self.client.put(url).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            204u16 => Ok(ResponseValue::empty(response)),
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `DELETE` request to `/0/users/{user}/privilege/{privilege}`"]
    pub async fn user_privilege_revoke<'a>(
        &'a self,
        user: &'a str,
        privilege: &'a str,
    ) -> Result<ResponseValue<()>, Error<types::Error>> {
        let url = format!(
            "{}/0/users/{}/privilege/{}",
            self.baseurl,
            progenitor_client::encode_path(&user.to_string()),
            progenitor_client::encode_path(&privilege.to_string()),
        );
        let request = self.client.delete(url).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            204u16 => Ok(ResponseValue::empty(response)),
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `GET` request to `/0/whoami`"]
    pub async fn whoami<'a>(
        &'a self,
    ) -> Result<ResponseValue<types::WhoamiResult>, Error<types::Error>> {
        let url = format!("{}/0/whoami", self.baseurl,);
        let request = self.client.get(url).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `POST` request to `/0/worker/bootstrap`"]
    pub async fn worker_bootstrap<'a>(
        &'a self,
        body: &'a types::WorkerBootstrap,
    ) -> Result<ResponseValue<types::WorkerBootstrapResult>, Error<types::Error>>
    {
        let url = format!("{}/0/worker/bootstrap", self.baseurl,);
        let request = self.client.post(url).json(body).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            201u16 => ResponseValue::from_response(response).await,
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `POST` request to `/0/worker/job/{job}/append`"]
    pub async fn worker_job_append<'a>(
        &'a self,
        job: &'a str,
        body: &'a types::WorkerAppendJob,
    ) -> Result<ResponseValue<()>, Error<types::Error>> {
        let url = format!(
            "{}/0/worker/job/{}/append",
            self.baseurl,
            progenitor_client::encode_path(&job.to_string()),
        );
        let request = self.client.post(url).json(body).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            204u16 => Ok(ResponseValue::empty(response)),
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `POST` request to `/0/worker/job/{job}/chunk`"]
    pub async fn worker_job_upload_chunk<'a, B: Into<reqwest::Body>>(
        &'a self,
        job: &'a str,
        body: B,
    ) -> Result<ResponseValue<types::UploadedChunk>, Error<types::Error>> {
        let url = format!(
            "{}/0/worker/job/{}/chunk",
            self.baseurl,
            progenitor_client::encode_path(&job.to_string()),
        );
        let request = self.client.post(url).body(body).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            201u16 => ResponseValue::from_response(response).await,
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `POST` request to `/0/worker/job/{job}/complete`"]
    pub async fn worker_job_complete<'a>(
        &'a self,
        job: &'a str,
        body: &'a types::WorkerCompleteJob,
    ) -> Result<ResponseValue<()>, Error<types::Error>> {
        let url = format!(
            "{}/0/worker/job/{}/complete",
            self.baseurl,
            progenitor_client::encode_path(&job.to_string()),
        );
        let request = self.client.post(url).json(body).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            204u16 => Ok(ResponseValue::empty(response)),
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `GET` request to `/0/worker/job/{job}/inputs/{input}`"]
    pub async fn worker_job_input_download<'a>(
        &'a self,
        job: &'a str,
        input: &'a str,
    ) -> Result<ResponseValue<ByteStream>, Error<ByteStream>> {
        let url = format!(
            "{}/0/worker/job/{}/inputs/{}",
            self.baseurl,
            progenitor_client::encode_path(&job.to_string()),
            progenitor_client::encode_path(&input.to_string()),
        );
        let request = self.client.get(url).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            200..=299 => Ok(ResponseValue::stream(response)),
            _ => Err(Error::ErrorResponse(ResponseValue::stream(response))),
        }
    }

    #[doc = "Sends a `POST` request to `/0/worker/job/{job}/output`"]
    pub async fn worker_job_add_output<'a>(
        &'a self,
        job: &'a str,
        body: &'a types::WorkerAddOutput,
    ) -> Result<ResponseValue<()>, Error<types::Error>> {
        let url = format!(
            "{}/0/worker/job/{}/output",
            self.baseurl,
            progenitor_client::encode_path(&job.to_string()),
        );
        let request = self.client.post(url).json(body).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            204u16 => Ok(ResponseValue::empty(response)),
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `POST` request to `/0/worker/job/{job}/task/{task}/append`"]
    pub async fn worker_task_append<'a>(
        &'a self,
        job: &'a str,
        task: u32,
        body: &'a types::WorkerAppendJob,
    ) -> Result<ResponseValue<()>, Error<types::Error>> {
        let url = format!(
            "{}/0/worker/job/{}/task/{}/append",
            self.baseurl,
            progenitor_client::encode_path(&job.to_string()),
            progenitor_client::encode_path(&task.to_string()),
        );
        let request = self.client.post(url).json(body).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            204u16 => Ok(ResponseValue::empty(response)),
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `POST` request to `/0/worker/job/{job}/task/{task}/complete`"]
    pub async fn worker_task_complete<'a>(
        &'a self,
        job: &'a str,
        task: u32,
        body: &'a types::WorkerCompleteTask,
    ) -> Result<ResponseValue<()>, Error<types::Error>> {
        let url = format!(
            "{}/0/worker/job/{}/task/{}/complete",
            self.baseurl,
            progenitor_client::encode_path(&job.to_string()),
            progenitor_client::encode_path(&task.to_string()),
        );
        let request = self.client.post(url).json(body).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            204u16 => Ok(ResponseValue::empty(response)),
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `GET` request to `/0/worker/ping`"]
    pub async fn worker_ping<'a>(
        &'a self,
    ) -> Result<ResponseValue<types::WorkerPingResult>, Error<types::Error>>
    {
        let url = format!("{}/0/worker/ping", self.baseurl,);
        let request = self.client.get(url).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `GET` request to `/0/workers`"]
    pub async fn workers_list<'a>(
        &'a self,
        active: Option<bool>,
    ) -> Result<ResponseValue<types::WorkersResult>, Error<types::Error>> {
        let url = format!("{}/0/workers", self.baseurl,);
        let mut query = Vec::new();
        if let Some(v) = &active {
            query.push(("active", v.to_string()));
        }

        let request = self.client.get(url).query(&query).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }

    #[doc = "Sends a `POST` request to `/0/workers/recycle`"]
    pub async fn workers_recycle<'a>(
        &'a self,
    ) -> Result<ResponseValue<()>, Error<types::Error>> {
        let url = format!("{}/0/workers/recycle", self.baseurl,);
        let request = self.client.post(url).build()?;
        let result = self.client.execute(request).await;
        let response = result?;
        match response.status().as_u16() {
            204u16 => Ok(ResponseValue::empty(response)),
            400u16..=499u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            500u16..=599u16 => Err(Error::ErrorResponse(
                ResponseValue::from_response(response).await?,
            )),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }
}
