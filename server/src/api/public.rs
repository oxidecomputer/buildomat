/*
 * Copyright 2024 Oxide Computer Company
 */

use super::prelude::*;

#[derive(Deserialize, JsonSchema)]
pub(crate) struct PublicFilePath {
    username: String,
    series: String,
    version: String,
    name: String,
}

#[endpoint {
    method = GET,
    path = "/0/public/file/{username}/{series}/{version}/{name}",
}]
pub(crate) async fn public_file_download(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<PublicFilePath>,
) -> DSResult<Response<Body>> {
    public_file_common(
        &rqctx.log,
        &path.into_inner(),
        rqctx.context(),
        rqctx.range(),
        false,
    )
    .await
}

#[endpoint {
    method = HEAD,
    path = "/0/public/file/{username}/{series}/{version}/{name}",
}]
pub(crate) async fn public_file_head(
    rqctx: RequestContext<Arc<Central>>,
    path: TypedPath<PublicFilePath>,
) -> DSResult<Response<Body>> {
    public_file_common(
        &rqctx.log,
        &path.into_inner(),
        rqctx.context(),
        rqctx.range(),
        true,
    )
    .await
}

pub(crate) async fn public_file_common(
    log: &Logger,
    p: &PublicFilePath,
    c: &Central,
    pr: Option<PotentialRange>,
    head_only: bool,
) -> DSResult<Response<Body>> {
    /*
     * Load the user from the database.
     */
    let u = if let Some(au) = c.db.user_by_name(&p.username).or_500()? {
        au.id
    } else {
        return Err(HttpError::for_client_error(
            None,
            ClientErrorStatusCode::NOT_FOUND,
            "published file not found".into(),
        ));
    };

    let pf = if let Some(pf) =
        c.db.published_file_by_name(u, &p.series, &p.version, &p.name)
            .or_500()?
    {
        pf
    } else {
        return Err(HttpError::for_client_error(
            None,
            ClientErrorStatusCode::NOT_FOUND,
            "published file not found".into(),
        ));
    };

    let info = format!(
        "published file: user {} series {} version {} name {}",
        u, pf.series, pf.version, pf.name
    );
    c.file_response(log, info, pf.job, pf.file, pr, head_only).await
}
