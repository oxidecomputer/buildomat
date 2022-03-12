use anyhow::Result;
mod progenitor_support {
    use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
    #[allow(dead_code)]
    const PATH_SET: &AsciiSet = &CONTROLS
        .add(b' ')
        .add(b'"')
        .add(b'#')
        .add(b'<')
        .add(b'>')
        .add(b'?')
        .add(b'`')
        .add(b'{')
        .add(b'}');
    #[allow(dead_code)]
    pub(crate) fn encode_path(pc: &str) -> String {
        utf8_percent_encode(pc, PATH_SET).to_string()
    }
}

pub mod types {
    use serde::{Deserialize, Serialize};
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
    pub struct FactoryWorkerAssociate {
        pub private: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct FactoryWorkerCreate {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub job: Option<String>,
        pub target: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct FactoryWorkerResult {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub worker: Option<FactoryWorker>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct Job {
        pub id: String,
        pub name: String,
        pub output_rules: Vec<String>,
        pub owner: String,
        pub state: String,
        pub tags: std::collections::HashMap<String, String>,
        pub target: String,
        pub target_real: String,
        pub tasks: Vec<Task>,
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
    pub struct JobSubmit {
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

    #[doc = "factory_create: POST /0/admin/factory"]
    pub async fn factory_create<'a>(
        &'a self,
        body: &'a types::FactoryCreate,
    ) -> Result<types::FactoryCreateResult> {
        let url = format!("{}/0/admin/factory", self.baseurl,);
        let request = self.client.post(url).json(body).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res.json().await?)
    }

    #[doc = "admin_jobs_get: GET /0/admin/jobs"]
    pub async fn admin_jobs_get<'a>(
        &'a self,
        active: Option<bool>,
        completed: Option<u64>,
    ) -> Result<Vec<types::Job>> {
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
        let res = result?.error_for_status()?;
        Ok(res.json().await?)
    }

    #[doc = "admin_job_get: GET /0/admin/jobs/{job}"]
    pub async fn admin_job_get<'a>(
        &'a self,
        job: &'a str,
    ) -> Result<types::Job> {
        let url = format!(
            "{}/0/admin/jobs/{}",
            self.baseurl,
            progenitor_support::encode_path(&job.to_string()),
        );
        let request = self.client.get(url).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res.json().await?)
    }

    #[doc = "target_create: POST /0/admin/target"]
    pub async fn target_create<'a>(
        &'a self,
        body: &'a types::TargetCreate,
    ) -> Result<types::TargetCreateResult> {
        let url = format!("{}/0/admin/target", self.baseurl,);
        let request = self.client.post(url).json(body).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res.json().await?)
    }

    #[doc = "targets_list: GET /0/admin/targets"]
    pub async fn targets_list<'a>(&'a self) -> Result<Vec<types::Target>> {
        let url = format!("{}/0/admin/targets", self.baseurl,);
        let request = self.client.get(url).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res.json().await?)
    }

    #[doc = "target_require_no_privilege: DELETE /0/admin/targets/{target}/require"]
    pub async fn target_require_no_privilege<'a>(
        &'a self,
        target: &'a str,
    ) -> Result<reqwest::Response> {
        let url = format!(
            "{}/0/admin/targets/{}/require",
            self.baseurl,
            progenitor_support::encode_path(&target.to_string()),
        );
        let request = self.client.delete(url).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res)
    }

    #[doc = "target_require_privilege: PUT /0/admin/targets/{target}/require/{privilege}"]
    pub async fn target_require_privilege<'a>(
        &'a self,
        target: &'a str,
        privilege: &'a str,
    ) -> Result<reqwest::Response> {
        let url = format!(
            "{}/0/admin/targets/{}/require/{}",
            self.baseurl,
            progenitor_support::encode_path(&target.to_string()),
            progenitor_support::encode_path(&privilege.to_string()),
        );
        let request = self.client.put(url).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res)
    }

    #[doc = "control_hold: POST /0/control/hold"]
    pub async fn control_hold<'a>(&'a self) -> Result<reqwest::Response> {
        let url = format!("{}/0/control/hold", self.baseurl,);
        let request = self.client.post(url).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res)
    }

    #[doc = "control_resume: POST /0/control/resume"]
    pub async fn control_resume<'a>(&'a self) -> Result<reqwest::Response> {
        let url = format!("{}/0/control/resume", self.baseurl,);
        let request = self.client.post(url).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res)
    }

    #[doc = "factory_lease: POST /0/factory/lease"]
    pub async fn factory_lease<'a>(
        &'a self,
        body: &'a types::FactoryWhatsNext,
    ) -> Result<types::FactoryLeaseResult> {
        let url = format!("{}/0/factory/lease", self.baseurl,);
        let request = self.client.post(url).json(body).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res.json().await?)
    }

    #[doc = "factory_lease_renew: POST /0/factory/lease/{job}"]
    pub async fn factory_lease_renew<'a>(
        &'a self,
        job: &'a str,
    ) -> Result<bool> {
        let url = format!(
            "{}/0/factory/lease/{}",
            self.baseurl,
            progenitor_support::encode_path(&job.to_string()),
        );
        let request = self.client.post(url).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res.json().await?)
    }

    #[doc = "factory_ping: GET /0/factory/ping"]
    pub async fn factory_ping<'a>(
        &'a self,
    ) -> Result<types::FactoryPingResult> {
        let url = format!("{}/0/factory/ping", self.baseurl,);
        let request = self.client.get(url).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res.json().await?)
    }

    #[doc = "factory_worker_create: POST /0/factory/worker"]
    pub async fn factory_worker_create<'a>(
        &'a self,
        body: &'a types::FactoryWorkerCreate,
    ) -> Result<types::FactoryWorker> {
        let url = format!("{}/0/factory/worker", self.baseurl,);
        let request = self.client.post(url).json(body).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res.json().await?)
    }

    #[doc = "factory_worker_get: GET /0/factory/worker/{worker}"]
    pub async fn factory_worker_get<'a>(
        &'a self,
        worker: &'a str,
    ) -> Result<types::FactoryWorkerResult> {
        let url = format!(
            "{}/0/factory/worker/{}",
            self.baseurl,
            progenitor_support::encode_path(&worker.to_string()),
        );
        let request = self.client.get(url).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res.json().await?)
    }

    #[doc = "factory_worker_destroy: DELETE /0/factory/worker/{worker}"]
    pub async fn factory_worker_destroy<'a>(
        &'a self,
        worker: &'a str,
    ) -> Result<bool> {
        let url = format!(
            "{}/0/factory/worker/{}",
            self.baseurl,
            progenitor_support::encode_path(&worker.to_string()),
        );
        let request = self.client.delete(url).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res.json().await?)
    }

    #[doc = "factory_worker_associate: PATCH /0/factory/worker/{worker}"]
    pub async fn factory_worker_associate<'a>(
        &'a self,
        worker: &'a str,
        body: &'a types::FactoryWorkerAssociate,
    ) -> Result<reqwest::Response> {
        let url = format!(
            "{}/0/factory/worker/{}",
            self.baseurl,
            progenitor_support::encode_path(&worker.to_string()),
        );
        let request = self.client.patch(url).json(body).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res)
    }

    #[doc = "factory_workers: GET /0/factory/workers"]
    pub async fn factory_workers<'a>(
        &'a self,
    ) -> Result<Vec<types::FactoryWorker>> {
        let url = format!("{}/0/factory/workers", self.baseurl,);
        let request = self.client.get(url).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res.json().await?)
    }

    #[doc = "job_get: GET /0/job/{job}"]
    pub async fn job_get<'a>(&'a self, job: &'a str) -> Result<types::Job> {
        let url = format!(
            "{}/0/job/{}",
            self.baseurl,
            progenitor_support::encode_path(&job.to_string()),
        );
        let request = self.client.get(url).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res.json().await?)
    }

    #[doc = "jobs_get: GET /0/jobs"]
    pub async fn jobs_get<'a>(&'a self) -> Result<Vec<types::Job>> {
        let url = format!("{}/0/jobs", self.baseurl,);
        let request = self.client.get(url).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res.json().await?)
    }

    #[doc = "job_submit: POST /0/jobs"]
    pub async fn job_submit<'a>(
        &'a self,
        body: &'a types::JobSubmit,
    ) -> Result<types::JobSubmitResult> {
        let url = format!("{}/0/jobs", self.baseurl,);
        let request = self.client.post(url).json(body).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res.json().await?)
    }

    #[doc = "job_upload_chunk: POST /0/jobs/{job}/chunk"]
    pub async fn job_upload_chunk<'a, B: Into<reqwest::Body>>(
        &'a self,
        job: &'a str,
        body: B,
    ) -> Result<types::UploadedChunk> {
        let url = format!(
            "{}/0/jobs/{}/chunk",
            self.baseurl,
            progenitor_support::encode_path(&job.to_string()),
        );
        let request = self.client.post(url).body(body).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res.json().await?)
    }

    #[doc = "job_events_get: GET /0/jobs/{job}/events"]
    pub async fn job_events_get<'a>(
        &'a self,
        job: &'a str,
        minseq: Option<u32>,
    ) -> Result<Vec<types::JobEvent>> {
        let url = format!(
            "{}/0/jobs/{}/events",
            self.baseurl,
            progenitor_support::encode_path(&job.to_string()),
        );
        let mut query = Vec::new();
        if let Some(v) = &minseq {
            query.push(("minseq", v.to_string()));
        }

        let request = self.client.get(url).query(&query).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res.json().await?)
    }

    #[doc = "job_add_input: POST /0/jobs/{job}/input"]
    pub async fn job_add_input<'a>(
        &'a self,
        job: &'a str,
        body: &'a types::JobAddInput,
    ) -> Result<reqwest::Response> {
        let url = format!(
            "{}/0/jobs/{}/input",
            self.baseurl,
            progenitor_support::encode_path(&job.to_string()),
        );
        let request = self.client.post(url).json(body).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res)
    }

    #[doc = "job_outputs_get: GET /0/jobs/{job}/outputs"]
    pub async fn job_outputs_get<'a>(
        &'a self,
        job: &'a str,
    ) -> Result<Vec<types::JobOutput>> {
        let url = format!(
            "{}/0/jobs/{}/outputs",
            self.baseurl,
            progenitor_support::encode_path(&job.to_string()),
        );
        let request = self.client.get(url).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res.json().await?)
    }

    #[doc = "job_output_download: GET /0/jobs/{job}/outputs/{output}"]
    pub async fn job_output_download<'a>(
        &'a self,
        job: &'a str,
        output: &'a str,
    ) -> Result<reqwest::Response> {
        let url = format!(
            "{}/0/jobs/{}/outputs/{}",
            self.baseurl,
            progenitor_support::encode_path(&job.to_string()),
            progenitor_support::encode_path(&output.to_string()),
        );
        let request = self.client.get(url).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res)
    }

    #[doc = "users_list: GET /0/users"]
    pub async fn users_list<'a>(&'a self) -> Result<Vec<types::User>> {
        let url = format!("{}/0/users", self.baseurl,);
        let request = self.client.get(url).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res.json().await?)
    }

    #[doc = "user_create: POST /0/users"]
    pub async fn user_create<'a>(
        &'a self,
        body: &'a types::UserCreate,
    ) -> Result<types::UserCreateResult> {
        let url = format!("{}/0/users", self.baseurl,);
        let request = self.client.post(url).json(body).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res.json().await?)
    }

    #[doc = "user_get: GET /0/users/{user}"]
    pub async fn user_get<'a>(&'a self, user: &'a str) -> Result<types::User> {
        let url = format!(
            "{}/0/users/{}",
            self.baseurl,
            progenitor_support::encode_path(&user.to_string()),
        );
        let request = self.client.get(url).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res.json().await?)
    }

    #[doc = "user_privilege_grant: PUT /0/users/{user}/privilege/{privilege}"]
    pub async fn user_privilege_grant<'a>(
        &'a self,
        user: &'a str,
        privilege: &'a str,
    ) -> Result<reqwest::Response> {
        let url = format!(
            "{}/0/users/{}/privilege/{}",
            self.baseurl,
            progenitor_support::encode_path(&user.to_string()),
            progenitor_support::encode_path(&privilege.to_string()),
        );
        let request = self.client.put(url).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res)
    }

    #[doc = "user_privilege_revoke: DELETE /0/users/{user}/privilege/{privilege}"]
    pub async fn user_privilege_revoke<'a>(
        &'a self,
        user: &'a str,
        privilege: &'a str,
    ) -> Result<reqwest::Response> {
        let url = format!(
            "{}/0/users/{}/privilege/{}",
            self.baseurl,
            progenitor_support::encode_path(&user.to_string()),
            progenitor_support::encode_path(&privilege.to_string()),
        );
        let request = self.client.delete(url).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res)
    }

    #[doc = "whoami: GET /0/whoami"]
    pub async fn whoami<'a>(&'a self) -> Result<types::WhoamiResult> {
        let url = format!("{}/0/whoami", self.baseurl,);
        let request = self.client.get(url).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res.json().await?)
    }

    #[doc = "worker_bootstrap: POST /0/worker/bootstrap"]
    pub async fn worker_bootstrap<'a>(
        &'a self,
        body: &'a types::WorkerBootstrap,
    ) -> Result<types::WorkerBootstrapResult> {
        let url = format!("{}/0/worker/bootstrap", self.baseurl,);
        let request = self.client.post(url).json(body).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res.json().await?)
    }

    #[doc = "worker_job_append: POST /0/worker/job/{job}/append"]
    pub async fn worker_job_append<'a>(
        &'a self,
        job: &'a str,
        body: &'a types::WorkerAppendJob,
    ) -> Result<reqwest::Response> {
        let url = format!(
            "{}/0/worker/job/{}/append",
            self.baseurl,
            progenitor_support::encode_path(&job.to_string()),
        );
        let request = self.client.post(url).json(body).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res)
    }

    #[doc = "worker_job_upload_chunk: POST /0/worker/job/{job}/chunk"]
    pub async fn worker_job_upload_chunk<'a, B: Into<reqwest::Body>>(
        &'a self,
        job: &'a str,
        body: B,
    ) -> Result<types::UploadedChunk> {
        let url = format!(
            "{}/0/worker/job/{}/chunk",
            self.baseurl,
            progenitor_support::encode_path(&job.to_string()),
        );
        let request = self.client.post(url).body(body).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res.json().await?)
    }

    #[doc = "worker_job_complete: POST /0/worker/job/{job}/complete"]
    pub async fn worker_job_complete<'a>(
        &'a self,
        job: &'a str,
        body: &'a types::WorkerCompleteJob,
    ) -> Result<reqwest::Response> {
        let url = format!(
            "{}/0/worker/job/{}/complete",
            self.baseurl,
            progenitor_support::encode_path(&job.to_string()),
        );
        let request = self.client.post(url).json(body).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res)
    }

    #[doc = "worker_job_input_download: GET /0/worker/job/{job}/inputs/{input}"]
    pub async fn worker_job_input_download<'a>(
        &'a self,
        job: &'a str,
        input: &'a str,
    ) -> Result<reqwest::Response> {
        let url = format!(
            "{}/0/worker/job/{}/inputs/{}",
            self.baseurl,
            progenitor_support::encode_path(&job.to_string()),
            progenitor_support::encode_path(&input.to_string()),
        );
        let request = self.client.get(url).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res)
    }

    #[doc = "worker_job_add_output: POST /0/worker/job/{job}/output"]
    pub async fn worker_job_add_output<'a>(
        &'a self,
        job: &'a str,
        body: &'a types::WorkerAddOutput,
    ) -> Result<reqwest::Response> {
        let url = format!(
            "{}/0/worker/job/{}/output",
            self.baseurl,
            progenitor_support::encode_path(&job.to_string()),
        );
        let request = self.client.post(url).json(body).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res)
    }

    #[doc = "worker_task_append: POST /0/worker/job/{job}/task/{task}/append"]
    pub async fn worker_task_append<'a>(
        &'a self,
        job: &'a str,
        task: u32,
        body: &'a types::WorkerAppendJob,
    ) -> Result<reqwest::Response> {
        let url = format!(
            "{}/0/worker/job/{}/task/{}/append",
            self.baseurl,
            progenitor_support::encode_path(&job.to_string()),
            progenitor_support::encode_path(&task.to_string()),
        );
        let request = self.client.post(url).json(body).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res)
    }

    #[doc = "worker_task_complete: POST /0/worker/job/{job}/task/{task}/complete"]
    pub async fn worker_task_complete<'a>(
        &'a self,
        job: &'a str,
        task: u32,
        body: &'a types::WorkerCompleteTask,
    ) -> Result<reqwest::Response> {
        let url = format!(
            "{}/0/worker/job/{}/task/{}/complete",
            self.baseurl,
            progenitor_support::encode_path(&job.to_string()),
            progenitor_support::encode_path(&task.to_string()),
        );
        let request = self.client.post(url).json(body).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res)
    }

    #[doc = "worker_ping: GET /0/worker/ping"]
    pub async fn worker_ping<'a>(&'a self) -> Result<types::WorkerPingResult> {
        let url = format!("{}/0/worker/ping", self.baseurl,);
        let request = self.client.get(url).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res.json().await?)
    }

    #[doc = "workers_list: GET /0/workers"]
    pub async fn workers_list<'a>(
        &'a self,
        active: Option<bool>,
    ) -> Result<types::WorkersResult> {
        let url = format!("{}/0/workers", self.baseurl,);
        let mut query = Vec::new();
        if let Some(v) = &active {
            query.push(("active", v.to_string()));
        }

        let request = self.client.get(url).query(&query).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res.json().await?)
    }

    #[doc = "workers_recycle: POST /0/workers/recycle"]
    pub async fn workers_recycle<'a>(&'a self) -> Result<reqwest::Response> {
        let url = format!("{}/0/workers/recycle", self.baseurl,);
        let request = self.client.post(url).build()?;
        let result = self.client.execute(request).await;
        let res = result?.error_for_status()?;
        Ok(res)
    }
}
