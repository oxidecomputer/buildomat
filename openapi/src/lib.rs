mod progenitor_client;

#[allow(unused_imports)]
use progenitor_client::encode_path;
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
        #[serde(default)]
        pub wait_for_flush: bool,
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
    pub(crate) baseurl: String,
    pub(crate) client: reqwest::Client,
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
}

impl Client {
    #[doc = "Sends a `POST` request to `/0/admin/factory`"]
    pub fn factory_create(&self) -> builder::FactoryCreate {
        builder::FactoryCreate::new(self)
    }

    #[doc = "Sends a `GET` request to `/0/admin/jobs`"]
    pub fn admin_jobs_get(&self) -> builder::AdminJobsGet {
        builder::AdminJobsGet::new(self)
    }

    #[doc = "Sends a `GET` request to `/0/admin/jobs/{job}`"]
    pub fn admin_job_get(&self) -> builder::AdminJobGet {
        builder::AdminJobGet::new(self)
    }

    #[doc = "Sends a `POST` request to `/0/admin/target`"]
    pub fn target_create(&self) -> builder::TargetCreate {
        builder::TargetCreate::new(self)
    }

    #[doc = "Sends a `GET` request to `/0/admin/targets`"]
    pub fn targets_list(&self) -> builder::TargetsList {
        builder::TargetsList::new(self)
    }

    #[doc = "Sends a `PUT` request to `/0/admin/targets/{target}/redirect`"]
    pub fn target_redirect(&self) -> builder::TargetRedirect {
        builder::TargetRedirect::new(self)
    }

    #[doc = "Sends a `POST` request to `/0/admin/targets/{target}/rename`"]
    pub fn target_rename(&self) -> builder::TargetRename {
        builder::TargetRename::new(self)
    }

    #[doc = "Sends a `DELETE` request to `/0/admin/targets/{target}/require`"]
    pub fn target_require_no_privilege(
        &self,
    ) -> builder::TargetRequireNoPrivilege {
        builder::TargetRequireNoPrivilege::new(self)
    }

    #[doc = "Sends a `PUT` request to `/0/admin/targets/{target}/require/{privilege}`"]
    pub fn target_require_privilege(&self) -> builder::TargetRequirePrivilege {
        builder::TargetRequirePrivilege::new(self)
    }

    #[doc = "Sends a `POST` request to `/0/control/hold`"]
    pub fn control_hold(&self) -> builder::ControlHold {
        builder::ControlHold::new(self)
    }

    #[doc = "Sends a `POST` request to `/0/control/resume`"]
    pub fn control_resume(&self) -> builder::ControlResume {
        builder::ControlResume::new(self)
    }

    #[doc = "Sends a `POST` request to `/0/factory/lease`"]
    pub fn factory_lease(&self) -> builder::FactoryLease {
        builder::FactoryLease::new(self)
    }

    #[doc = "Sends a `POST` request to `/0/factory/lease/{job}`"]
    pub fn factory_lease_renew(&self) -> builder::FactoryLeaseRenew {
        builder::FactoryLeaseRenew::new(self)
    }

    #[doc = "Sends a `GET` request to `/0/factory/ping`"]
    pub fn factory_ping(&self) -> builder::FactoryPing {
        builder::FactoryPing::new(self)
    }

    #[doc = "Sends a `POST` request to `/0/factory/worker`"]
    pub fn factory_worker_create(&self) -> builder::FactoryWorkerCreate {
        builder::FactoryWorkerCreate::new(self)
    }

    #[doc = "Sends a `GET` request to `/0/factory/worker/{worker}`"]
    pub fn factory_worker_get(&self) -> builder::FactoryWorkerGet {
        builder::FactoryWorkerGet::new(self)
    }

    #[doc = "Sends a `DELETE` request to `/0/factory/worker/{worker}`"]
    pub fn factory_worker_destroy(&self) -> builder::FactoryWorkerDestroy {
        builder::FactoryWorkerDestroy::new(self)
    }

    #[doc = "Sends a `PATCH` request to `/0/factory/worker/{worker}`"]
    pub fn factory_worker_associate(&self) -> builder::FactoryWorkerAssociate {
        builder::FactoryWorkerAssociate::new(self)
    }

    #[doc = "Sends a `POST` request to `/0/factory/worker/{worker}/append`"]
    pub fn factory_worker_append(&self) -> builder::FactoryWorkerAppend {
        builder::FactoryWorkerAppend::new(self)
    }

    #[doc = "Sends a `POST` request to `/0/factory/worker/{worker}/flush`"]
    pub fn factory_worker_flush(&self) -> builder::FactoryWorkerFlush {
        builder::FactoryWorkerFlush::new(self)
    }

    #[doc = "Sends a `GET` request to `/0/factory/workers`"]
    pub fn factory_workers(&self) -> builder::FactoryWorkers {
        builder::FactoryWorkers::new(self)
    }

    #[doc = "Sends a `GET` request to `/0/job/{job}`"]
    pub fn job_get(&self) -> builder::JobGet {
        builder::JobGet::new(self)
    }

    #[doc = "Sends a `GET` request to `/0/jobs`"]
    pub fn jobs_get(&self) -> builder::JobsGet {
        builder::JobsGet::new(self)
    }

    #[doc = "Sends a `POST` request to `/0/jobs`"]
    pub fn job_submit(&self) -> builder::JobSubmit {
        builder::JobSubmit::new(self)
    }

    #[doc = "Sends a `POST` request to `/0/jobs/{job}/cancel`"]
    pub fn job_cancel(&self) -> builder::JobCancel {
        builder::JobCancel::new(self)
    }

    #[doc = "Sends a `POST` request to `/0/jobs/{job}/chunk`"]
    pub fn job_upload_chunk(&self) -> builder::JobUploadChunk {
        builder::JobUploadChunk::new(self)
    }

    #[doc = "Sends a `GET` request to `/0/jobs/{job}/events`"]
    pub fn job_events_get(&self) -> builder::JobEventsGet {
        builder::JobEventsGet::new(self)
    }

    #[doc = "Sends a `POST` request to `/0/jobs/{job}/input`"]
    pub fn job_add_input(&self) -> builder::JobAddInput {
        builder::JobAddInput::new(self)
    }

    #[doc = "Sends a `GET` request to `/0/jobs/{job}/outputs`"]
    pub fn job_outputs_get(&self) -> builder::JobOutputsGet {
        builder::JobOutputsGet::new(self)
    }

    #[doc = "Sends a `GET` request to `/0/jobs/{job}/outputs/{output}`"]
    pub fn job_output_download(&self) -> builder::JobOutputDownload {
        builder::JobOutputDownload::new(self)
    }

    #[doc = "Sends a `POST` request to `/0/jobs/{job}/outputs/{output}/publish`"]
    pub fn job_output_publish(&self) -> builder::JobOutputPublish {
        builder::JobOutputPublish::new(self)
    }

    #[doc = "Sends a `GET` request to `/0/public/file/{username}/{series}/{version}/{name}`"]
    pub fn public_file_download(&self) -> builder::PublicFileDownload {
        builder::PublicFileDownload::new(self)
    }

    #[doc = "Sends a `GET` request to `/0/users`"]
    pub fn users_list(&self) -> builder::UsersList {
        builder::UsersList::new(self)
    }

    #[doc = "Sends a `POST` request to `/0/users`"]
    pub fn user_create(&self) -> builder::UserCreate {
        builder::UserCreate::new(self)
    }

    #[doc = "Sends a `GET` request to `/0/users/{user}`"]
    pub fn user_get(&self) -> builder::UserGet {
        builder::UserGet::new(self)
    }

    #[doc = "Sends a `PUT` request to `/0/users/{user}/privilege/{privilege}`"]
    pub fn user_privilege_grant(&self) -> builder::UserPrivilegeGrant {
        builder::UserPrivilegeGrant::new(self)
    }

    #[doc = "Sends a `DELETE` request to `/0/users/{user}/privilege/{privilege}`"]
    pub fn user_privilege_revoke(&self) -> builder::UserPrivilegeRevoke {
        builder::UserPrivilegeRevoke::new(self)
    }

    #[doc = "Sends a `GET` request to `/0/whoami`"]
    pub fn whoami(&self) -> builder::Whoami {
        builder::Whoami::new(self)
    }

    #[doc = "Sends a `POST` request to `/0/worker/bootstrap`"]
    pub fn worker_bootstrap(&self) -> builder::WorkerBootstrap {
        builder::WorkerBootstrap::new(self)
    }

    #[doc = "Sends a `POST` request to `/0/worker/job/{job}/append`"]
    pub fn worker_job_append(&self) -> builder::WorkerJobAppend {
        builder::WorkerJobAppend::new(self)
    }

    #[doc = "Sends a `POST` request to `/0/worker/job/{job}/chunk`"]
    pub fn worker_job_upload_chunk(&self) -> builder::WorkerJobUploadChunk {
        builder::WorkerJobUploadChunk::new(self)
    }

    #[doc = "Sends a `POST` request to `/0/worker/job/{job}/complete`"]
    pub fn worker_job_complete(&self) -> builder::WorkerJobComplete {
        builder::WorkerJobComplete::new(self)
    }

    #[doc = "Sends a `GET` request to `/0/worker/job/{job}/inputs/{input}`"]
    pub fn worker_job_input_download(&self) -> builder::WorkerJobInputDownload {
        builder::WorkerJobInputDownload::new(self)
    }

    #[doc = "Sends a `POST` request to `/0/worker/job/{job}/output`"]
    pub fn worker_job_add_output(&self) -> builder::WorkerJobAddOutput {
        builder::WorkerJobAddOutput::new(self)
    }

    #[doc = "Sends a `POST` request to `/0/worker/job/{job}/task/{task}/append`"]
    pub fn worker_task_append(&self) -> builder::WorkerTaskAppend {
        builder::WorkerTaskAppend::new(self)
    }

    #[doc = "Sends a `POST` request to `/0/worker/job/{job}/task/{task}/complete`"]
    pub fn worker_task_complete(&self) -> builder::WorkerTaskComplete {
        builder::WorkerTaskComplete::new(self)
    }

    #[doc = "Sends a `GET` request to `/0/worker/ping`"]
    pub fn worker_ping(&self) -> builder::WorkerPing {
        builder::WorkerPing::new(self)
    }

    #[doc = "Sends a `GET` request to `/0/workers`"]
    pub fn workers_list(&self) -> builder::WorkersList {
        builder::WorkersList::new(self)
    }

    #[doc = "Sends a `POST` request to `/0/workers/recycle`"]
    pub fn workers_recycle(&self) -> builder::WorkersRecycle {
        builder::WorkersRecycle::new(self)
    }
}

pub mod builder {
    #[allow(unused_imports)]
    use super::encode_path;
    use super::types;
    #[allow(unused_imports)]
    use super::{ByteStream, Error, ResponseValue};
    #[derive(Clone)]
    pub struct FactoryCreate<'a> {
        client: &'a super::Client,
        body: Option<types::FactoryCreate>,
    }

    impl<'a> FactoryCreate<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, body: None }
        }

        pub fn body(mut self, value: types::FactoryCreate) -> Self {
            self.body = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<
            ResponseValue<types::FactoryCreateResult>,
            Error<types::Error>,
        > {
            let Self { client, body } = self;
            let (body,) = match (body,) {
                (Some(body),) => (body,),
                (body,) => {
                    let mut missing = Vec::new();
                    if body.is_none() {
                        missing.push(stringify!(body));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!("{}/0/admin/factory", client.baseurl,);
            let request = client.client.post(url).json(&body).build()?;
            let result = client.client.execute(request).await;
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
    }

    #[derive(Clone)]
    pub struct AdminJobsGet<'a> {
        client: &'a super::Client,
        active: Option<bool>,
        completed: Option<u64>,
    }

    impl<'a> AdminJobsGet<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, active: None, completed: None }
        }

        pub fn active(mut self, value: bool) -> Self {
            self.active = Some(value);
            self
        }

        pub fn completed(mut self, value: u64) -> Self {
            self.completed = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<Vec<types::Job>>, Error<types::Error>>
        {
            let Self { client, active, completed } = self;
            let url = format!("{}/0/admin/jobs", client.baseurl,);
            let mut query = Vec::new();
            if let Some(v) = &active {
                query.push(("active", v.to_string()));
            }
            if let Some(v) = &completed {
                query.push(("completed", v.to_string()));
            }
            let request = client.client.get(url).query(&query).build()?;
            let result = client.client.execute(request).await;
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
    }

    #[derive(Clone)]
    pub struct AdminJobGet<'a> {
        client: &'a super::Client,
        job: Option<String>,
    }

    impl<'a> AdminJobGet<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, job: None }
        }

        pub fn job(mut self, value: String) -> Self {
            self.job = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<types::Job>, Error<types::Error>> {
            let Self { client, job } = self;
            let (job,) = match (job,) {
                (Some(job),) => (job,),
                (job,) => {
                    let mut missing = Vec::new();
                    if job.is_none() {
                        missing.push(stringify!(job));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!(
                "{}/0/admin/jobs/{}",
                client.baseurl,
                encode_path(&job.to_string()),
            );
            let request = client.client.get(url).build()?;
            let result = client.client.execute(request).await;
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
    }

    #[derive(Clone)]
    pub struct TargetCreate<'a> {
        client: &'a super::Client,
        body: Option<types::TargetCreate>,
    }

    impl<'a> TargetCreate<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, body: None }
        }

        pub fn body(mut self, value: types::TargetCreate) -> Self {
            self.body = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<types::TargetCreateResult>, Error<types::Error>>
        {
            let Self { client, body } = self;
            let (body,) = match (body,) {
                (Some(body),) => (body,),
                (body,) => {
                    let mut missing = Vec::new();
                    if body.is_none() {
                        missing.push(stringify!(body));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!("{}/0/admin/target", client.baseurl,);
            let request = client.client.post(url).json(&body).build()?;
            let result = client.client.execute(request).await;
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
    }

    #[derive(Clone)]
    pub struct TargetsList<'a> {
        client: &'a super::Client,
    }

    impl<'a> TargetsList<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client }
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<Vec<types::Target>>, Error<types::Error>>
        {
            let Self { client } = self;
            let url = format!("{}/0/admin/targets", client.baseurl,);
            let request = client.client.get(url).build()?;
            let result = client.client.execute(request).await;
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
    }

    #[derive(Clone)]
    pub struct TargetRedirect<'a> {
        client: &'a super::Client,
        target: Option<String>,
        body: Option<types::TargetRedirect>,
    }

    impl<'a> TargetRedirect<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, target: None, body: None }
        }

        pub fn target(mut self, value: String) -> Self {
            self.target = Some(value);
            self
        }

        pub fn body(mut self, value: types::TargetRedirect) -> Self {
            self.body = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<()>, Error<types::Error>> {
            let Self { client, target, body } = self;
            let (target, body) = match (target, body) {
                (Some(target), Some(body)) => (target, body),
                (target, body) => {
                    let mut missing = Vec::new();
                    if target.is_none() {
                        missing.push(stringify!(target));
                    }
                    if body.is_none() {
                        missing.push(stringify!(body));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!(
                "{}/0/admin/targets/{}/redirect",
                client.baseurl,
                encode_path(&target.to_string()),
            );
            let request = client.client.put(url).json(&body).build()?;
            let result = client.client.execute(request).await;
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

    #[derive(Clone)]
    pub struct TargetRename<'a> {
        client: &'a super::Client,
        target: Option<String>,
        body: Option<types::TargetRename>,
    }

    impl<'a> TargetRename<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, target: None, body: None }
        }

        pub fn target(mut self, value: String) -> Self {
            self.target = Some(value);
            self
        }

        pub fn body(mut self, value: types::TargetRename) -> Self {
            self.body = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<types::TargetCreateResult>, Error<types::Error>>
        {
            let Self { client, target, body } = self;
            let (target, body) = match (target, body) {
                (Some(target), Some(body)) => (target, body),
                (target, body) => {
                    let mut missing = Vec::new();
                    if target.is_none() {
                        missing.push(stringify!(target));
                    }
                    if body.is_none() {
                        missing.push(stringify!(body));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!(
                "{}/0/admin/targets/{}/rename",
                client.baseurl,
                encode_path(&target.to_string()),
            );
            let request = client.client.post(url).json(&body).build()?;
            let result = client.client.execute(request).await;
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
    }

    #[derive(Clone)]
    pub struct TargetRequireNoPrivilege<'a> {
        client: &'a super::Client,
        target: Option<String>,
    }

    impl<'a> TargetRequireNoPrivilege<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, target: None }
        }

        pub fn target(mut self, value: String) -> Self {
            self.target = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<()>, Error<types::Error>> {
            let Self { client, target } = self;
            let (target,) = match (target,) {
                (Some(target),) => (target,),
                (target,) => {
                    let mut missing = Vec::new();
                    if target.is_none() {
                        missing.push(stringify!(target));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!(
                "{}/0/admin/targets/{}/require",
                client.baseurl,
                encode_path(&target.to_string()),
            );
            let request = client.client.delete(url).build()?;
            let result = client.client.execute(request).await;
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

    #[derive(Clone)]
    pub struct TargetRequirePrivilege<'a> {
        client: &'a super::Client,
        target: Option<String>,
        privilege: Option<String>,
    }

    impl<'a> TargetRequirePrivilege<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, target: None, privilege: None }
        }

        pub fn target(mut self, value: String) -> Self {
            self.target = Some(value);
            self
        }

        pub fn privilege(mut self, value: String) -> Self {
            self.privilege = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<()>, Error<types::Error>> {
            let Self { client, target, privilege } = self;
            let (target, privilege) = match (target, privilege) {
                (Some(target), Some(privilege)) => (target, privilege),
                (target, privilege) => {
                    let mut missing = Vec::new();
                    if target.is_none() {
                        missing.push(stringify!(target));
                    }
                    if privilege.is_none() {
                        missing.push(stringify!(privilege));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!(
                "{}/0/admin/targets/{}/require/{}",
                client.baseurl,
                encode_path(&target.to_string()),
                encode_path(&privilege.to_string()),
            );
            let request = client.client.put(url).build()?;
            let result = client.client.execute(request).await;
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

    #[derive(Clone)]
    pub struct ControlHold<'a> {
        client: &'a super::Client,
    }

    impl<'a> ControlHold<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client }
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<()>, Error<types::Error>> {
            let Self { client } = self;
            let url = format!("{}/0/control/hold", client.baseurl,);
            let request = client.client.post(url).build()?;
            let result = client.client.execute(request).await;
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

    #[derive(Clone)]
    pub struct ControlResume<'a> {
        client: &'a super::Client,
    }

    impl<'a> ControlResume<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client }
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<()>, Error<types::Error>> {
            let Self { client } = self;
            let url = format!("{}/0/control/resume", client.baseurl,);
            let request = client.client.post(url).build()?;
            let result = client.client.execute(request).await;
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

    #[derive(Clone)]
    pub struct FactoryLease<'a> {
        client: &'a super::Client,
        body: Option<types::FactoryWhatsNext>,
    }

    impl<'a> FactoryLease<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, body: None }
        }

        pub fn body(mut self, value: types::FactoryWhatsNext) -> Self {
            self.body = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<types::FactoryLeaseResult>, Error<types::Error>>
        {
            let Self { client, body } = self;
            let (body,) = match (body,) {
                (Some(body),) => (body,),
                (body,) => {
                    let mut missing = Vec::new();
                    if body.is_none() {
                        missing.push(stringify!(body));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!("{}/0/factory/lease", client.baseurl,);
            let request = client.client.post(url).json(&body).build()?;
            let result = client.client.execute(request).await;
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
    }

    #[derive(Clone)]
    pub struct FactoryLeaseRenew<'a> {
        client: &'a super::Client,
        job: Option<String>,
    }

    impl<'a> FactoryLeaseRenew<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, job: None }
        }

        pub fn job(mut self, value: String) -> Self {
            self.job = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<bool>, Error<types::Error>> {
            let Self { client, job } = self;
            let (job,) = match (job,) {
                (Some(job),) => (job,),
                (job,) => {
                    let mut missing = Vec::new();
                    if job.is_none() {
                        missing.push(stringify!(job));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!(
                "{}/0/factory/lease/{}",
                client.baseurl,
                encode_path(&job.to_string()),
            );
            let request = client.client.post(url).build()?;
            let result = client.client.execute(request).await;
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
    }

    #[derive(Clone)]
    pub struct FactoryPing<'a> {
        client: &'a super::Client,
    }

    impl<'a> FactoryPing<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client }
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<types::FactoryPingResult>, Error<types::Error>>
        {
            let Self { client } = self;
            let url = format!("{}/0/factory/ping", client.baseurl,);
            let request = client.client.get(url).build()?;
            let result = client.client.execute(request).await;
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
    }

    #[derive(Clone)]
    pub struct FactoryWorkerCreate<'a> {
        client: &'a super::Client,
        body: Option<types::FactoryWorkerCreate>,
    }

    impl<'a> FactoryWorkerCreate<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, body: None }
        }

        pub fn body(mut self, value: types::FactoryWorkerCreate) -> Self {
            self.body = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<types::FactoryWorker>, Error<types::Error>>
        {
            let Self { client, body } = self;
            let (body,) = match (body,) {
                (Some(body),) => (body,),
                (body,) => {
                    let mut missing = Vec::new();
                    if body.is_none() {
                        missing.push(stringify!(body));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!("{}/0/factory/worker", client.baseurl,);
            let request = client.client.post(url).json(&body).build()?;
            let result = client.client.execute(request).await;
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
    }

    #[derive(Clone)]
    pub struct FactoryWorkerGet<'a> {
        client: &'a super::Client,
        worker: Option<String>,
    }

    impl<'a> FactoryWorkerGet<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, worker: None }
        }

        pub fn worker(mut self, value: String) -> Self {
            self.worker = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<
            ResponseValue<types::FactoryWorkerResult>,
            Error<types::Error>,
        > {
            let Self { client, worker } = self;
            let (worker,) = match (worker,) {
                (Some(worker),) => (worker,),
                (worker,) => {
                    let mut missing = Vec::new();
                    if worker.is_none() {
                        missing.push(stringify!(worker));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!(
                "{}/0/factory/worker/{}",
                client.baseurl,
                encode_path(&worker.to_string()),
            );
            let request = client.client.get(url).build()?;
            let result = client.client.execute(request).await;
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
    }

    #[derive(Clone)]
    pub struct FactoryWorkerDestroy<'a> {
        client: &'a super::Client,
        worker: Option<String>,
    }

    impl<'a> FactoryWorkerDestroy<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, worker: None }
        }

        pub fn worker(mut self, value: String) -> Self {
            self.worker = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<bool>, Error<types::Error>> {
            let Self { client, worker } = self;
            let (worker,) = match (worker,) {
                (Some(worker),) => (worker,),
                (worker,) => {
                    let mut missing = Vec::new();
                    if worker.is_none() {
                        missing.push(stringify!(worker));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!(
                "{}/0/factory/worker/{}",
                client.baseurl,
                encode_path(&worker.to_string()),
            );
            let request = client.client.delete(url).build()?;
            let result = client.client.execute(request).await;
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
    }

    #[derive(Clone)]
    pub struct FactoryWorkerAssociate<'a> {
        client: &'a super::Client,
        worker: Option<String>,
        body: Option<types::FactoryWorkerAssociate>,
    }

    impl<'a> FactoryWorkerAssociate<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, worker: None, body: None }
        }

        pub fn worker(mut self, value: String) -> Self {
            self.worker = Some(value);
            self
        }

        pub fn body(mut self, value: types::FactoryWorkerAssociate) -> Self {
            self.body = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<()>, Error<types::Error>> {
            let Self { client, worker, body } = self;
            let (worker, body) = match (worker, body) {
                (Some(worker), Some(body)) => (worker, body),
                (worker, body) => {
                    let mut missing = Vec::new();
                    if worker.is_none() {
                        missing.push(stringify!(worker));
                    }
                    if body.is_none() {
                        missing.push(stringify!(body));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!(
                "{}/0/factory/worker/{}",
                client.baseurl,
                encode_path(&worker.to_string()),
            );
            let request = client.client.patch(url).json(&body).build()?;
            let result = client.client.execute(request).await;
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

    #[derive(Clone)]
    pub struct FactoryWorkerAppend<'a> {
        client: &'a super::Client,
        worker: Option<String>,
        body: Option<types::FactoryWorkerAppend>,
    }

    impl<'a> FactoryWorkerAppend<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, worker: None, body: None }
        }

        pub fn worker(mut self, value: String) -> Self {
            self.worker = Some(value);
            self
        }

        pub fn body(mut self, value: types::FactoryWorkerAppend) -> Self {
            self.body = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<
            ResponseValue<types::FactoryWorkerAppendResult>,
            Error<types::Error>,
        > {
            let Self { client, worker, body } = self;
            let (worker, body) = match (worker, body) {
                (Some(worker), Some(body)) => (worker, body),
                (worker, body) => {
                    let mut missing = Vec::new();
                    if worker.is_none() {
                        missing.push(stringify!(worker));
                    }
                    if body.is_none() {
                        missing.push(stringify!(body));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!(
                "{}/0/factory/worker/{}/append",
                client.baseurl,
                encode_path(&worker.to_string()),
            );
            let request = client.client.post(url).json(&body).build()?;
            let result = client.client.execute(request).await;
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
    }

    #[derive(Clone)]
    pub struct FactoryWorkerFlush<'a> {
        client: &'a super::Client,
        worker: Option<String>,
    }

    impl<'a> FactoryWorkerFlush<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, worker: None }
        }

        pub fn worker(mut self, value: String) -> Self {
            self.worker = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<()>, Error<types::Error>> {
            let Self { client, worker } = self;
            let (worker,) = match (worker,) {
                (Some(worker),) => (worker,),
                (worker,) => {
                    let mut missing = Vec::new();
                    if worker.is_none() {
                        missing.push(stringify!(worker));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!(
                "{}/0/factory/worker/{}/flush",
                client.baseurl,
                encode_path(&worker.to_string()),
            );
            let request = client.client.post(url).build()?;
            let result = client.client.execute(request).await;
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

    #[derive(Clone)]
    pub struct FactoryWorkers<'a> {
        client: &'a super::Client,
    }

    impl<'a> FactoryWorkers<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client }
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<Vec<types::FactoryWorker>>, Error<types::Error>>
        {
            let Self { client } = self;
            let url = format!("{}/0/factory/workers", client.baseurl,);
            let request = client.client.get(url).build()?;
            let result = client.client.execute(request).await;
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
    }

    #[derive(Clone)]
    pub struct JobGet<'a> {
        client: &'a super::Client,
        job: Option<String>,
    }

    impl<'a> JobGet<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, job: None }
        }

        pub fn job(mut self, value: String) -> Self {
            self.job = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<types::Job>, Error<types::Error>> {
            let Self { client, job } = self;
            let (job,) = match (job,) {
                (Some(job),) => (job,),
                (job,) => {
                    let mut missing = Vec::new();
                    if job.is_none() {
                        missing.push(stringify!(job));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!(
                "{}/0/job/{}",
                client.baseurl,
                encode_path(&job.to_string()),
            );
            let request = client.client.get(url).build()?;
            let result = client.client.execute(request).await;
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
    }

    #[derive(Clone)]
    pub struct JobsGet<'a> {
        client: &'a super::Client,
    }

    impl<'a> JobsGet<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client }
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<Vec<types::Job>>, Error<types::Error>>
        {
            let Self { client } = self;
            let url = format!("{}/0/jobs", client.baseurl,);
            let request = client.client.get(url).build()?;
            let result = client.client.execute(request).await;
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
    }

    #[derive(Clone)]
    pub struct JobSubmit<'a> {
        client: &'a super::Client,
        body: Option<types::JobSubmit>,
    }

    impl<'a> JobSubmit<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, body: None }
        }

        pub fn body(mut self, value: types::JobSubmit) -> Self {
            self.body = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<types::JobSubmitResult>, Error<types::Error>>
        {
            let Self { client, body } = self;
            let (body,) = match (body,) {
                (Some(body),) => (body,),
                (body,) => {
                    let mut missing = Vec::new();
                    if body.is_none() {
                        missing.push(stringify!(body));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!("{}/0/jobs", client.baseurl,);
            let request = client.client.post(url).json(&body).build()?;
            let result = client.client.execute(request).await;
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
    }

    #[derive(Clone)]
    pub struct JobCancel<'a> {
        client: &'a super::Client,
        job: Option<String>,
    }

    impl<'a> JobCancel<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, job: None }
        }

        pub fn job(mut self, value: String) -> Self {
            self.job = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<()>, Error<types::Error>> {
            let Self { client, job } = self;
            let (job,) = match (job,) {
                (Some(job),) => (job,),
                (job,) => {
                    let mut missing = Vec::new();
                    if job.is_none() {
                        missing.push(stringify!(job));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!(
                "{}/0/jobs/{}/cancel",
                client.baseurl,
                encode_path(&job.to_string()),
            );
            let request = client.client.post(url).build()?;
            let result = client.client.execute(request).await;
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

    pub struct JobUploadChunk<'a> {
        client: &'a super::Client,
        job: Option<String>,
        body: Option<reqwest::Body>,
    }

    impl<'a> JobUploadChunk<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, job: None, body: None }
        }

        pub fn job(mut self, value: String) -> Self {
            self.job = Some(value);
            self
        }

        pub fn body<B: Into<reqwest::Body>>(mut self, value: B) -> Self {
            self.body = Some(value.into());
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<types::UploadedChunk>, Error<types::Error>>
        {
            let Self { client, job, body } = self;
            let (job, body) = match (job, body) {
                (Some(job), Some(body)) => (job, body),
                (job, body) => {
                    let mut missing = Vec::new();
                    if job.is_none() {
                        missing.push(stringify!(job));
                    }
                    if body.is_none() {
                        missing.push(stringify!(body));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!(
                "{}/0/jobs/{}/chunk",
                client.baseurl,
                encode_path(&job.to_string()),
            );
            let request = client.client.post(url).body(body).build()?;
            let result = client.client.execute(request).await;
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
    }

    #[derive(Clone)]
    pub struct JobEventsGet<'a> {
        client: &'a super::Client,
        job: Option<String>,
        minseq: Option<u32>,
    }

    impl<'a> JobEventsGet<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, job: None, minseq: None }
        }

        pub fn job(mut self, value: String) -> Self {
            self.job = Some(value);
            self
        }

        pub fn minseq(mut self, value: u32) -> Self {
            self.minseq = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<Vec<types::JobEvent>>, Error<types::Error>>
        {
            let Self { client, job, minseq } = self;
            let (job,) = match (job,) {
                (Some(job),) => (job,),
                (job,) => {
                    let mut missing = Vec::new();
                    if job.is_none() {
                        missing.push(stringify!(job));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!(
                "{}/0/jobs/{}/events",
                client.baseurl,
                encode_path(&job.to_string()),
            );
            let mut query = Vec::new();
            if let Some(v) = &minseq {
                query.push(("minseq", v.to_string()));
            }
            let request = client.client.get(url).query(&query).build()?;
            let result = client.client.execute(request).await;
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
    }

    #[derive(Clone)]
    pub struct JobAddInput<'a> {
        client: &'a super::Client,
        job: Option<String>,
        body: Option<types::JobAddInput>,
    }

    impl<'a> JobAddInput<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, job: None, body: None }
        }

        pub fn job(mut self, value: String) -> Self {
            self.job = Some(value);
            self
        }

        pub fn body(mut self, value: types::JobAddInput) -> Self {
            self.body = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<()>, Error<types::Error>> {
            let Self { client, job, body } = self;
            let (job, body) = match (job, body) {
                (Some(job), Some(body)) => (job, body),
                (job, body) => {
                    let mut missing = Vec::new();
                    if job.is_none() {
                        missing.push(stringify!(job));
                    }
                    if body.is_none() {
                        missing.push(stringify!(body));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!(
                "{}/0/jobs/{}/input",
                client.baseurl,
                encode_path(&job.to_string()),
            );
            let request = client.client.post(url).json(&body).build()?;
            let result = client.client.execute(request).await;
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

    #[derive(Clone)]
    pub struct JobOutputsGet<'a> {
        client: &'a super::Client,
        job: Option<String>,
    }

    impl<'a> JobOutputsGet<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, job: None }
        }

        pub fn job(mut self, value: String) -> Self {
            self.job = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<Vec<types::JobOutput>>, Error<types::Error>>
        {
            let Self { client, job } = self;
            let (job,) = match (job,) {
                (Some(job),) => (job,),
                (job,) => {
                    let mut missing = Vec::new();
                    if job.is_none() {
                        missing.push(stringify!(job));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!(
                "{}/0/jobs/{}/outputs",
                client.baseurl,
                encode_path(&job.to_string()),
            );
            let request = client.client.get(url).build()?;
            let result = client.client.execute(request).await;
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
    }

    #[derive(Clone)]
    pub struct JobOutputDownload<'a> {
        client: &'a super::Client,
        job: Option<String>,
        output: Option<String>,
    }

    impl<'a> JobOutputDownload<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, job: None, output: None }
        }

        pub fn job(mut self, value: String) -> Self {
            self.job = Some(value);
            self
        }

        pub fn output(mut self, value: String) -> Self {
            self.output = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<ByteStream>, Error<ByteStream>> {
            let Self { client, job, output } = self;
            let (job, output) = match (job, output) {
                (Some(job), Some(output)) => (job, output),
                (job, output) => {
                    let mut missing = Vec::new();
                    if job.is_none() {
                        missing.push(stringify!(job));
                    }
                    if output.is_none() {
                        missing.push(stringify!(output));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!(
                "{}/0/jobs/{}/outputs/{}",
                client.baseurl,
                encode_path(&job.to_string()),
                encode_path(&output.to_string()),
            );
            let request = client.client.get(url).build()?;
            let result = client.client.execute(request).await;
            let response = result?;
            match response.status().as_u16() {
                200..=299 => Ok(ResponseValue::stream(response)),
                _ => Err(Error::ErrorResponse(ResponseValue::stream(response))),
            }
        }
    }

    #[derive(Clone)]
    pub struct JobOutputPublish<'a> {
        client: &'a super::Client,
        job: Option<String>,
        output: Option<String>,
        body: Option<types::JobOutputPublish>,
    }

    impl<'a> JobOutputPublish<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, job: None, output: None, body: None }
        }

        pub fn job(mut self, value: String) -> Self {
            self.job = Some(value);
            self
        }

        pub fn output(mut self, value: String) -> Self {
            self.output = Some(value);
            self
        }

        pub fn body(mut self, value: types::JobOutputPublish) -> Self {
            self.body = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<()>, Error<types::Error>> {
            let Self { client, job, output, body } = self;
            let (job, output, body) = match (job, output, body) {
                (Some(job), Some(output), Some(body)) => (job, output, body),
                (job, output, body) => {
                    let mut missing = Vec::new();
                    if job.is_none() {
                        missing.push(stringify!(job));
                    }
                    if output.is_none() {
                        missing.push(stringify!(output));
                    }
                    if body.is_none() {
                        missing.push(stringify!(body));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!(
                "{}/0/jobs/{}/outputs/{}/publish",
                client.baseurl,
                encode_path(&job.to_string()),
                encode_path(&output.to_string()),
            );
            let request = client.client.post(url).json(&body).build()?;
            let result = client.client.execute(request).await;
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

    #[derive(Clone)]
    pub struct PublicFileDownload<'a> {
        client: &'a super::Client,
        username: Option<String>,
        series: Option<String>,
        version: Option<String>,
        name: Option<String>,
    }

    impl<'a> PublicFileDownload<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self {
                client,
                username: None,
                series: None,
                version: None,
                name: None,
            }
        }

        pub fn username(mut self, value: String) -> Self {
            self.username = Some(value);
            self
        }

        pub fn series(mut self, value: String) -> Self {
            self.series = Some(value);
            self
        }

        pub fn version(mut self, value: String) -> Self {
            self.version = Some(value);
            self
        }

        pub fn name(mut self, value: String) -> Self {
            self.name = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<ByteStream>, Error<ByteStream>> {
            let Self { client, username, series, version, name } = self;
            let (username, series, version, name) =
                match (username, series, version, name) {
                    (
                        Some(username),
                        Some(series),
                        Some(version),
                        Some(name),
                    ) => (username, series, version, name),
                    (username, series, version, name) => {
                        let mut missing = Vec::new();
                        if username.is_none() {
                            missing.push(stringify!(username));
                        }
                        if series.is_none() {
                            missing.push(stringify!(series));
                        }
                        if version.is_none() {
                            missing.push(stringify!(version));
                        }
                        if name.is_none() {
                            missing.push(stringify!(name));
                        }
                        return Err(super::Error::InvalidRequest(format!(
                            "the following parameters are required: {}",
                            missing.join(", "),
                        )));
                    }
                };
            let url = format!(
                "{}/0/public/file/{}/{}/{}/{}",
                client.baseurl,
                encode_path(&username.to_string()),
                encode_path(&series.to_string()),
                encode_path(&version.to_string()),
                encode_path(&name.to_string()),
            );
            let request = client.client.get(url).build()?;
            let result = client.client.execute(request).await;
            let response = result?;
            match response.status().as_u16() {
                200..=299 => Ok(ResponseValue::stream(response)),
                _ => Err(Error::ErrorResponse(ResponseValue::stream(response))),
            }
        }
    }

    #[derive(Clone)]
    pub struct UsersList<'a> {
        client: &'a super::Client,
    }

    impl<'a> UsersList<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client }
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<Vec<types::User>>, Error<types::Error>>
        {
            let Self { client } = self;
            let url = format!("{}/0/users", client.baseurl,);
            let request = client.client.get(url).build()?;
            let result = client.client.execute(request).await;
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
    }

    #[derive(Clone)]
    pub struct UserCreate<'a> {
        client: &'a super::Client,
        body: Option<types::UserCreate>,
    }

    impl<'a> UserCreate<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, body: None }
        }

        pub fn body(mut self, value: types::UserCreate) -> Self {
            self.body = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<types::UserCreateResult>, Error<types::Error>>
        {
            let Self { client, body } = self;
            let (body,) = match (body,) {
                (Some(body),) => (body,),
                (body,) => {
                    let mut missing = Vec::new();
                    if body.is_none() {
                        missing.push(stringify!(body));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!("{}/0/users", client.baseurl,);
            let request = client.client.post(url).json(&body).build()?;
            let result = client.client.execute(request).await;
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
    }

    #[derive(Clone)]
    pub struct UserGet<'a> {
        client: &'a super::Client,
        user: Option<String>,
    }

    impl<'a> UserGet<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, user: None }
        }

        pub fn user(mut self, value: String) -> Self {
            self.user = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<types::User>, Error<types::Error>> {
            let Self { client, user } = self;
            let (user,) = match (user,) {
                (Some(user),) => (user,),
                (user,) => {
                    let mut missing = Vec::new();
                    if user.is_none() {
                        missing.push(stringify!(user));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!(
                "{}/0/users/{}",
                client.baseurl,
                encode_path(&user.to_string()),
            );
            let request = client.client.get(url).build()?;
            let result = client.client.execute(request).await;
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
    }

    #[derive(Clone)]
    pub struct UserPrivilegeGrant<'a> {
        client: &'a super::Client,
        user: Option<String>,
        privilege: Option<String>,
    }

    impl<'a> UserPrivilegeGrant<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, user: None, privilege: None }
        }

        pub fn user(mut self, value: String) -> Self {
            self.user = Some(value);
            self
        }

        pub fn privilege(mut self, value: String) -> Self {
            self.privilege = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<()>, Error<types::Error>> {
            let Self { client, user, privilege } = self;
            let (user, privilege) = match (user, privilege) {
                (Some(user), Some(privilege)) => (user, privilege),
                (user, privilege) => {
                    let mut missing = Vec::new();
                    if user.is_none() {
                        missing.push(stringify!(user));
                    }
                    if privilege.is_none() {
                        missing.push(stringify!(privilege));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!(
                "{}/0/users/{}/privilege/{}",
                client.baseurl,
                encode_path(&user.to_string()),
                encode_path(&privilege.to_string()),
            );
            let request = client.client.put(url).build()?;
            let result = client.client.execute(request).await;
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

    #[derive(Clone)]
    pub struct UserPrivilegeRevoke<'a> {
        client: &'a super::Client,
        user: Option<String>,
        privilege: Option<String>,
    }

    impl<'a> UserPrivilegeRevoke<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, user: None, privilege: None }
        }

        pub fn user(mut self, value: String) -> Self {
            self.user = Some(value);
            self
        }

        pub fn privilege(mut self, value: String) -> Self {
            self.privilege = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<()>, Error<types::Error>> {
            let Self { client, user, privilege } = self;
            let (user, privilege) = match (user, privilege) {
                (Some(user), Some(privilege)) => (user, privilege),
                (user, privilege) => {
                    let mut missing = Vec::new();
                    if user.is_none() {
                        missing.push(stringify!(user));
                    }
                    if privilege.is_none() {
                        missing.push(stringify!(privilege));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!(
                "{}/0/users/{}/privilege/{}",
                client.baseurl,
                encode_path(&user.to_string()),
                encode_path(&privilege.to_string()),
            );
            let request = client.client.delete(url).build()?;
            let result = client.client.execute(request).await;
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

    #[derive(Clone)]
    pub struct Whoami<'a> {
        client: &'a super::Client,
    }

    impl<'a> Whoami<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client }
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<types::WhoamiResult>, Error<types::Error>>
        {
            let Self { client } = self;
            let url = format!("{}/0/whoami", client.baseurl,);
            let request = client.client.get(url).build()?;
            let result = client.client.execute(request).await;
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
    }

    #[derive(Clone)]
    pub struct WorkerBootstrap<'a> {
        client: &'a super::Client,
        body: Option<types::WorkerBootstrap>,
    }

    impl<'a> WorkerBootstrap<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, body: None }
        }

        pub fn body(mut self, value: types::WorkerBootstrap) -> Self {
            self.body = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<
            ResponseValue<types::WorkerBootstrapResult>,
            Error<types::Error>,
        > {
            let Self { client, body } = self;
            let (body,) = match (body,) {
                (Some(body),) => (body,),
                (body,) => {
                    let mut missing = Vec::new();
                    if body.is_none() {
                        missing.push(stringify!(body));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!("{}/0/worker/bootstrap", client.baseurl,);
            let request = client.client.post(url).json(&body).build()?;
            let result = client.client.execute(request).await;
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
    }

    #[derive(Clone)]
    pub struct WorkerJobAppend<'a> {
        client: &'a super::Client,
        job: Option<String>,
        body: Option<types::WorkerAppendJob>,
    }

    impl<'a> WorkerJobAppend<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, job: None, body: None }
        }

        pub fn job(mut self, value: String) -> Self {
            self.job = Some(value);
            self
        }

        pub fn body(mut self, value: types::WorkerAppendJob) -> Self {
            self.body = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<()>, Error<types::Error>> {
            let Self { client, job, body } = self;
            let (job, body) = match (job, body) {
                (Some(job), Some(body)) => (job, body),
                (job, body) => {
                    let mut missing = Vec::new();
                    if job.is_none() {
                        missing.push(stringify!(job));
                    }
                    if body.is_none() {
                        missing.push(stringify!(body));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!(
                "{}/0/worker/job/{}/append",
                client.baseurl,
                encode_path(&job.to_string()),
            );
            let request = client.client.post(url).json(&body).build()?;
            let result = client.client.execute(request).await;
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

    pub struct WorkerJobUploadChunk<'a> {
        client: &'a super::Client,
        job: Option<String>,
        body: Option<reqwest::Body>,
    }

    impl<'a> WorkerJobUploadChunk<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, job: None, body: None }
        }

        pub fn job(mut self, value: String) -> Self {
            self.job = Some(value);
            self
        }

        pub fn body<B: Into<reqwest::Body>>(mut self, value: B) -> Self {
            self.body = Some(value.into());
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<types::UploadedChunk>, Error<types::Error>>
        {
            let Self { client, job, body } = self;
            let (job, body) = match (job, body) {
                (Some(job), Some(body)) => (job, body),
                (job, body) => {
                    let mut missing = Vec::new();
                    if job.is_none() {
                        missing.push(stringify!(job));
                    }
                    if body.is_none() {
                        missing.push(stringify!(body));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!(
                "{}/0/worker/job/{}/chunk",
                client.baseurl,
                encode_path(&job.to_string()),
            );
            let request = client.client.post(url).body(body).build()?;
            let result = client.client.execute(request).await;
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
    }

    #[derive(Clone)]
    pub struct WorkerJobComplete<'a> {
        client: &'a super::Client,
        job: Option<String>,
        body: Option<types::WorkerCompleteJob>,
    }

    impl<'a> WorkerJobComplete<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, job: None, body: None }
        }

        pub fn job(mut self, value: String) -> Self {
            self.job = Some(value);
            self
        }

        pub fn body(mut self, value: types::WorkerCompleteJob) -> Self {
            self.body = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<()>, Error<types::Error>> {
            let Self { client, job, body } = self;
            let (job, body) = match (job, body) {
                (Some(job), Some(body)) => (job, body),
                (job, body) => {
                    let mut missing = Vec::new();
                    if job.is_none() {
                        missing.push(stringify!(job));
                    }
                    if body.is_none() {
                        missing.push(stringify!(body));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!(
                "{}/0/worker/job/{}/complete",
                client.baseurl,
                encode_path(&job.to_string()),
            );
            let request = client.client.post(url).json(&body).build()?;
            let result = client.client.execute(request).await;
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

    #[derive(Clone)]
    pub struct WorkerJobInputDownload<'a> {
        client: &'a super::Client,
        job: Option<String>,
        input: Option<String>,
    }

    impl<'a> WorkerJobInputDownload<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, job: None, input: None }
        }

        pub fn job(mut self, value: String) -> Self {
            self.job = Some(value);
            self
        }

        pub fn input(mut self, value: String) -> Self {
            self.input = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<ByteStream>, Error<ByteStream>> {
            let Self { client, job, input } = self;
            let (job, input) = match (job, input) {
                (Some(job), Some(input)) => (job, input),
                (job, input) => {
                    let mut missing = Vec::new();
                    if job.is_none() {
                        missing.push(stringify!(job));
                    }
                    if input.is_none() {
                        missing.push(stringify!(input));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!(
                "{}/0/worker/job/{}/inputs/{}",
                client.baseurl,
                encode_path(&job.to_string()),
                encode_path(&input.to_string()),
            );
            let request = client.client.get(url).build()?;
            let result = client.client.execute(request).await;
            let response = result?;
            match response.status().as_u16() {
                200..=299 => Ok(ResponseValue::stream(response)),
                _ => Err(Error::ErrorResponse(ResponseValue::stream(response))),
            }
        }
    }

    #[derive(Clone)]
    pub struct WorkerJobAddOutput<'a> {
        client: &'a super::Client,
        job: Option<String>,
        body: Option<types::WorkerAddOutput>,
    }

    impl<'a> WorkerJobAddOutput<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, job: None, body: None }
        }

        pub fn job(mut self, value: String) -> Self {
            self.job = Some(value);
            self
        }

        pub fn body(mut self, value: types::WorkerAddOutput) -> Self {
            self.body = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<()>, Error<types::Error>> {
            let Self { client, job, body } = self;
            let (job, body) = match (job, body) {
                (Some(job), Some(body)) => (job, body),
                (job, body) => {
                    let mut missing = Vec::new();
                    if job.is_none() {
                        missing.push(stringify!(job));
                    }
                    if body.is_none() {
                        missing.push(stringify!(body));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!(
                "{}/0/worker/job/{}/output",
                client.baseurl,
                encode_path(&job.to_string()),
            );
            let request = client.client.post(url).json(&body).build()?;
            let result = client.client.execute(request).await;
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

    #[derive(Clone)]
    pub struct WorkerTaskAppend<'a> {
        client: &'a super::Client,
        job: Option<String>,
        task: Option<u32>,
        body: Option<types::WorkerAppendJob>,
    }

    impl<'a> WorkerTaskAppend<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, job: None, task: None, body: None }
        }

        pub fn job(mut self, value: String) -> Self {
            self.job = Some(value);
            self
        }

        pub fn task(mut self, value: u32) -> Self {
            self.task = Some(value);
            self
        }

        pub fn body(mut self, value: types::WorkerAppendJob) -> Self {
            self.body = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<()>, Error<types::Error>> {
            let Self { client, job, task, body } = self;
            let (job, task, body) = match (job, task, body) {
                (Some(job), Some(task), Some(body)) => (job, task, body),
                (job, task, body) => {
                    let mut missing = Vec::new();
                    if job.is_none() {
                        missing.push(stringify!(job));
                    }
                    if task.is_none() {
                        missing.push(stringify!(task));
                    }
                    if body.is_none() {
                        missing.push(stringify!(body));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!(
                "{}/0/worker/job/{}/task/{}/append",
                client.baseurl,
                encode_path(&job.to_string()),
                encode_path(&task.to_string()),
            );
            let request = client.client.post(url).json(&body).build()?;
            let result = client.client.execute(request).await;
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

    #[derive(Clone)]
    pub struct WorkerTaskComplete<'a> {
        client: &'a super::Client,
        job: Option<String>,
        task: Option<u32>,
        body: Option<types::WorkerCompleteTask>,
    }

    impl<'a> WorkerTaskComplete<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, job: None, task: None, body: None }
        }

        pub fn job(mut self, value: String) -> Self {
            self.job = Some(value);
            self
        }

        pub fn task(mut self, value: u32) -> Self {
            self.task = Some(value);
            self
        }

        pub fn body(mut self, value: types::WorkerCompleteTask) -> Self {
            self.body = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<()>, Error<types::Error>> {
            let Self { client, job, task, body } = self;
            let (job, task, body) = match (job, task, body) {
                (Some(job), Some(task), Some(body)) => (job, task, body),
                (job, task, body) => {
                    let mut missing = Vec::new();
                    if job.is_none() {
                        missing.push(stringify!(job));
                    }
                    if task.is_none() {
                        missing.push(stringify!(task));
                    }
                    if body.is_none() {
                        missing.push(stringify!(body));
                    }
                    return Err(super::Error::InvalidRequest(format!(
                        "the following parameters are required: {}",
                        missing.join(", "),
                    )));
                }
            };
            let url = format!(
                "{}/0/worker/job/{}/task/{}/complete",
                client.baseurl,
                encode_path(&job.to_string()),
                encode_path(&task.to_string()),
            );
            let request = client.client.post(url).json(&body).build()?;
            let result = client.client.execute(request).await;
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

    #[derive(Clone)]
    pub struct WorkerPing<'a> {
        client: &'a super::Client,
    }

    impl<'a> WorkerPing<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client }
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<types::WorkerPingResult>, Error<types::Error>>
        {
            let Self { client } = self;
            let url = format!("{}/0/worker/ping", client.baseurl,);
            let request = client.client.get(url).build()?;
            let result = client.client.execute(request).await;
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
    }

    #[derive(Clone)]
    pub struct WorkersList<'a> {
        client: &'a super::Client,
        active: Option<bool>,
    }

    impl<'a> WorkersList<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client, active: None }
        }

        pub fn active(mut self, value: bool) -> Self {
            self.active = Some(value);
            self
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<types::WorkersResult>, Error<types::Error>>
        {
            let Self { client, active } = self;
            let url = format!("{}/0/workers", client.baseurl,);
            let mut query = Vec::new();
            if let Some(v) = &active {
                query.push(("active", v.to_string()));
            }
            let request = client.client.get(url).query(&query).build()?;
            let result = client.client.execute(request).await;
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
    }

    #[derive(Clone)]
    pub struct WorkersRecycle<'a> {
        client: &'a super::Client,
    }

    impl<'a> WorkersRecycle<'a> {
        pub fn new(client: &'a super::Client) -> Self {
            Self { client }
        }

        pub async fn send(
            self,
        ) -> Result<ResponseValue<()>, Error<types::Error>> {
            let Self { client } = self;
            let url = format!("{}/0/workers/recycle", client.baseurl,);
            let request = client.client.post(url).build()?;
            let result = client.client.execute(request).await;
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
}
