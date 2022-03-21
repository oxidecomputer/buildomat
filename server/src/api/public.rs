/*
 * Copyright 2021 Oxide Computer Company
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
    rqctx: Arc<RequestContext<Arc<Central>>>,
    path: TypedPath<PublicFilePath>,
) -> std::result::Result<Response<Body>, HttpError> {
    let c = rqctx.context();
    let log = &rqctx.log;

    let p = path.into_inner();

    /*
     * Load the user from the database.
     */
    let u = if let Some(au) = c.db.user_get_by_name(&p.username).or_500()? {
        au.id
    } else {
        return Err(HttpError::for_client_error(
            None,
            StatusCode::NOT_FOUND,
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
            StatusCode::NOT_FOUND,
            "published file not found".into(),
        ));
    };

    let mut res = Response::builder();
    res = res.header(CONTENT_TYPE, "application/octet-stream");

    let fr = c.file_response(pf.job, pf.file).await.or_500()?;
    info!(
        log,
        "published file: user {} series {} version {} name {} is in the {}",
        u,
        pf.series,
        pf.version,
        pf.name,
        fr.info,
    );

    res = res.header(CONTENT_LENGTH, fr.size);
    Ok(res.body(fr.body)?)
}
