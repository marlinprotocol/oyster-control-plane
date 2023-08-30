use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};

use crate::market::{GBRateCard, InfraProvider, RegionalRates};

enum Error {
    GetIPFail,
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

#[derive(Debug, Serialize)]
struct BandwidthResponse {
    rates: Vec<GBRateCard>,
}

async fn get_ip(
    client: &(impl InfraProvider + Send + Sync + Clone),
    job_id: &str,
    region: String,
) -> Result<String> {
    let ip = client.get_job_ip(job_id, region).await?;

    Ok(ip)
}

async fn handle_ip_request(
    State(state): State<
        Arc<(
            impl InfraProvider + Send + Sync + Clone,
            Vec<String>,
            &'static [RegionalRates],
            &'static [GBRateCard],
        )>,
    >,
    Query(query): Query<GetIPRequest>,
) -> HandlerResult<Json<GetIPResponse>> {
    if !query.id.is_some() || !query.region.is_some() {
        return Err(Error::GetIPFail);
    }

    let client = &state.0;

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
            &'static [RegionalRates],
            &'static [GBRateCard],
        )>,
    >,
) -> HandlerResult<Json<SpecResponse>> {
    let regions = &state.1;
    let rates = state.2;

    let res = SpecResponse {
        allowed_regions: regions.to_owned(),
        min_rates: rates.to_owned(),
    };

    return Ok(Json(res));
}

async fn handle_bandwidth_request(
    State(state): State<
        Arc<(
            impl InfraProvider + Send + Sync + Clone,
            Vec<String>,
            &'static [RegionalRates],
            &'static [GBRateCard],
        )>,
    >,
) -> HandlerResult<Json<BandwidthResponse>> {
    let bandwidth = state.3;
    let res = BandwidthResponse {
        rates: bandwidth.to_owned(),
    };

    return Ok(Json(res));
}

fn all_routes(
    state: Arc<(
        impl InfraProvider + Send + Sync + Clone + 'static,
        Vec<String>,
        &'static [RegionalRates],
        &'static [GBRateCard],
    )>,
) -> Router {
    Router::new()
        .route("/ip", get(handle_ip_request))
        .route("/spec", get(handle_spec_request))
        .route("/bandwidth", get(handle_bandwidth_request))
        .with_state(state)
}

pub async fn serve(
    client: impl InfraProvider + Send + Sync + Clone + 'static,
    regions: Vec<String>,
    rates: &'static [RegionalRates],
    bandwidth: &'static [GBRateCard],
) {
    let state = Arc::from((client, regions, rates, bandwidth));

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

        for id in 1..4 {
            let temp_job_id = H256::from_low_u64_be(id).encode_hex();
            let instance_metadata = InstanceMetadata::new(None, None).await;

            aws.instances
                .insert(temp_job_id.clone(), instance_metadata.clone());
        }

        let job_id = H256::from_low_u64_be(1).encode_hex();
        let region = "ap-south-1".to_string();

        let res = get_ip(&aws, &job_id, region).await;
        assert!(res.is_ok());

        let res = res.unwrap();

        let actual_ip = &aws.instances.get_key_value(&job_id).unwrap().1.ip_address;
        assert_eq!(&res, actual_ip)
    }

    #[tokio::test]
    async fn test_get_ip_bad_case() {
        let mut aws: TestAws = Default::default();

        for id in 1..4 {
            let temp_job_id = H256::from_low_u64_be(id).encode_hex();
            let instance_metadata = InstanceMetadata::new(None, None).await;

            aws.instances
                .insert(temp_job_id.clone(), instance_metadata.clone());
        }

        let job_id = H256::from_low_u64_be(5).encode_hex();
        let region = "ap-south-1".to_string();

        let res = get_ip(&aws, &job_id, region).await;
        assert!(res.is_err());

        let err = res.as_ref().unwrap_err().to_string();
        assert_eq!(err, format!("Instance not found for job - {job_id}"));
    }

    #[tokio::test]
    async fn test_get_ip_bad_case_no_instances() {
        let aws: TestAws = Default::default();

        let job_id = H256::from_low_u64_be(1).encode_hex();
        let region = "ap-south-1".to_string();

        let res = get_ip(&aws, &job_id, region).await;
        assert!(res.is_err());

        let err = res.as_ref().unwrap_err().to_string();
        assert_eq!(err, format!("Instance not found for job - {job_id}"));
    }
}
