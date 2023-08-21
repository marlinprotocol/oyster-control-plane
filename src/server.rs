use std::{fs, net::SocketAddr, sync::Arc};

use anyhow::{anyhow, Result};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};

use crate::market::{InfraProvider, RegionalRates};

enum Error {
    GetIPFail,
    GetSpecFail,
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        (StatusCode::BAD_REQUEST, "BAD_REQUEST").into_response()
    }
}

type HandlerResult<T> = core::result::Result<T, Error>;

#[derive(Debug, Deserialize)]
struct GetIPRequest {
    id: Option<String>,
    region: Option<String>,
}

#[derive(Debug, Serialize)]
struct GetIPResponse {
    ip: String,
}

#[derive(Debug, Serialize)]
struct SpecResponse {
    allowed_regions: Vec<String>,
    min_rates: Vec<RegionalRates>,
}

async fn get_ip(
    mut client: impl InfraProvider + Send + Sync + Clone,
    job_id: &str,
    region: String,
) -> Result<String> {
    let instance = client.get_job_instance(job_id, region.clone()).await?;

    if !instance.0 {
        return Err(anyhow!("Instance not found for job - {job_id}"));
    }

    let ip = client.get_ip_from_instance_id(&instance.1, region).await?;

    Ok(ip)
}

async fn handle_ip_request(
    State(state): State<
        Arc<(
            impl InfraProvider + Send + Sync + Clone,
            Vec<String>,
            String,
        )>,
    >,
    Query(query): Query<GetIPRequest>,
) -> HandlerResult<Json<GetIPResponse>> {
    let client = state.0.clone();

    if !query.id.is_some() || !query.region.is_some() {
        return Err(Error::GetIPFail);
    }

    let ip = get_ip(client, &query.id.unwrap(), query.region.unwrap()).await;
    if ip.is_err() {
        return Err(Error::GetIPFail);
    }
    let ip = ip.unwrap().to_string();
    let ip = GetIPResponse { ip };

    Ok(Json(ip))
}

async fn handle_spec_request(
    State(state): State<
        Arc<(
            impl InfraProvider + Send + Sync + Clone,
            Vec<String>,
            String,
        )>,
    >,
) -> HandlerResult<Json<SpecResponse>> {
    let regions = &state.1;
    let rates_path = &state.2;

    let contents = fs::read_to_string(rates_path);

    if let Err(err) = contents {
        println!("Server: Error reading rates file: {err:?}");
    } else {
        let contents = contents.unwrap();
        let data: Vec<RegionalRates> = serde_json::from_str(&contents).unwrap_or_default();
        if !data.is_empty() {
            let res = SpecResponse {
                allowed_regions: regions.to_owned(),
                min_rates: data,
            };

            return Ok(Json(res));
        }
    }
    return Err(Error::GetSpecFail);
}

fn all_routes<'a>(
    state: Arc<(
        impl InfraProvider + Send + Sync + Clone + 'static,
        Vec<String>,
        String,
    )>,
) -> Router {
    Router::new()
        .route("/ip", get(handle_ip_request))
        .route("/spec", get(handle_spec_request))
        .with_state(state)
}

pub async fn serve(
    client: impl InfraProvider + Send + Sync + Clone + 'static,
    regions: Vec<String>,
    rates_path: String,
) {
    let state = Arc::from((client, regions, rates_path));

    let router = Router::new().merge(all_routes(state));

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    println!("Listening for connections on {}", addr);
    axum::Server::bind(&addr)
        .serve(router.into_make_service())
        .await
        .unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    use ethers::{abi::AbiEncode, prelude::*};

    use crate::test::{InstanceMetadata, TestAws};

    #[tokio::test]
    async fn test_get_ip_happy_case() {
        let mut aws: TestAws = Default::default();

        let job_id = H256::from_low_u64_be(1).encode_hex();
        let region = "ap-south-1".to_string();

        let instance_metadata = InstanceMetadata::new(None, None).await;

        aws.instances
            .insert(job_id.clone(), instance_metadata.clone());

        let res = get_ip(aws, &job_id, region).await;
        assert!(res.is_ok());

        let res = res.unwrap();
        assert_eq!(res, instance_metadata.ip_address)
    }

    #[tokio::test]
    async fn test_get_ip_bad_case() {
        let aws: TestAws = Default::default();

        let job_id = H256::from_low_u64_be(1).encode_hex();
        let region = "ap-south-1".to_string();

        let res = get_ip(aws, &job_id, region).await;
        assert!(res.is_err());

        let err = res.as_ref().unwrap_err().to_string();
        assert_eq!(err, format!("Instance not found for job - {job_id}"));
    }
}
