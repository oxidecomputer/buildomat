use super::prelude::*;

#[derive(Serialize, JsonSchema)]
pub struct Project {
}

#[endpoint {
    method = GET,
    path = "/0/projects",
}]
pub(crate) async fn projects_list(
    rc: Arc<RequestContext<Arc<App>>>,
) -> DSResult<HttpResponseOk<Vec<Project>>> {
    todo!()
}
