use actix_web::{
    http::StatusCode,
    post,
    web::{Data, Json},
    HttpRequest, Responder, ResponseError,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;

use crate::db::{self, pipelines::PipelineConfig};

#[derive(Debug, Error)]
enum PipelineError {
    #[error("database error: {0}")]
    DatabaseError(#[from] sqlx::Error),

    // #[error("sink with id {0} not found")]
    // NotFound(i64),
    #[error("tenant id missing in request")]
    TenantIdMissing,

    #[error("tenant id ill formed in request")]
    TenantIdIllFormed,

    #[error("invalid sink config")]
    InvalidConfig(#[from] serde_json::Error),
}

impl ResponseError for PipelineError {
    fn status_code(&self) -> StatusCode {
        match self {
            PipelineError::DatabaseError(_) | PipelineError::InvalidConfig(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            // PipelineError::NotFound(_) => StatusCode::NOT_FOUND,
            PipelineError::TenantIdMissing | PipelineError::TenantIdIllFormed => {
                StatusCode::BAD_REQUEST
            }
        }
    }
}

#[derive(Deserialize)]
struct PostPipelineRequest {
    pub source_id: i64,
    pub sink_id: i64,
    pub config: PipelineConfig,
}

#[derive(Serialize)]
struct PostPipelineResponse {
    id: i64,
}

// #[derive(Serialize)]
// struct GetPipelineResponse {
//     id: i64,
//     tenant_id: i64,
//     source_id: i64,
//     sink_id: i64,
//     config: PipelineConfig,
// }

// TODO: read tenant_id from a jwt
fn extract_tenant_id(req: &HttpRequest) -> Result<i64, PipelineError> {
    let headers = req.headers();
    let tenant_id = headers
        .get("tenant_id")
        .ok_or(PipelineError::TenantIdMissing)?;
    let tenant_id = tenant_id
        .to_str()
        .map_err(|_| PipelineError::TenantIdIllFormed)?;
    let tenant_id: i64 = tenant_id
        .parse()
        .map_err(|_| PipelineError::TenantIdIllFormed)?;
    Ok(tenant_id)
}

#[post("/pipelines")]
pub async fn create_pipeline(
    req: HttpRequest,
    pool: Data<PgPool>,
    pipeline: Json<PostPipelineRequest>,
) -> Result<impl Responder, PipelineError> {
    let pipeline = pipeline.0;
    let tenant_id = extract_tenant_id(&req)?;
    let config = pipeline.config;
    let id = db::pipelines::create_pipeline(
        &pool,
        tenant_id,
        pipeline.source_id,
        pipeline.sink_id,
        &config,
    )
    .await?;
    let response = PostPipelineResponse { id };
    Ok(Json(response))
}
