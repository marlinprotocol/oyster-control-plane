use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc};

use crate::{
    aws::PRC,
    market::{GBRateCard, InfraProvider, RegionalRates},
};

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
    enclave_state: String,
    enclave_eif_prc: PRC,
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

    let job_id = query.id.unwrap();
    let region = query.region.unwrap();

    let ip = client.get_job_ip(&job_id, region.clone()).await;

    if ip.is_err() {
        return Err(Error::GetIPFail);
    }
    let ip = ip.unwrap();

    let enclave_data = client.get_job_enclave_state(&job_id, region).await;

    if enclave_data.is_err() {
        return Err(Error::GetIPFail);
    }
    let enclave_data: (String, PRC) = enclave_data.unwrap();

    let res = GetIPResponse {
        ip,
        enclave_state: enclave_data.0,
        enclave_eif_prc: enclave_data.1,
    };

    Ok(Json(res))
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
    port: Option<u16>,
) {
    let state = Arc::from((client, regions, rates, bandwidth));

    let router = Router::new().merge(all_routes(state));
    let port = port.unwrap_or(8080);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    println!("Listening for connections on {}", addr);
    axum::Server::bind(&addr)
        .serve(router.into_make_service())
        .await
        .unwrap();
}

#[cfg(test)]
mod tests {
    use super::serve;

    use anyhow;
    use ethers::{abi::AbiEncode, prelude::*};
    use serde_json::json;

    use crate::market::{GBRateCard, RateCard, RegionalRates};
    use crate::test::{InstanceMetadata, TestAws};

    #[tokio::test]
    async fn test_get_ip_happy_case() -> anyhow::Result<()> {
        let mut aws: TestAws = Default::default();

        for id in 1..4 {
            let temp_job_id = H256::from_low_u64_be(id).encode_hex();
            let instance_metadata = InstanceMetadata::new(None, None, None).await;

            aws.instances
                .insert(temp_job_id.clone(), instance_metadata.clone());
        }

        let regions: Vec<String> = vec![String::from("ap-south-1")];
        let compute_rates: &'static [RegionalRates] = Box::leak(vec![].into_boxed_slice());
        let bandwidth_rates: &'static [GBRateCard] = Box::leak(vec![].into_boxed_slice());
        let port = Some(8081);

        tokio::spawn(serve(
            aws.clone(),
            regions,
            compute_rates,
            bandwidth_rates,
            port,
        ));

        let hc = httpc_test::new_client(format!("http://localhost:{}", port.unwrap()))?;

        let job_id = H256::from_low_u64_be(1).encode_hex();

        let res = hc
            .do_get(&format!("/ip?id={}&region=ap-south-1", job_id))
            .await?;
        assert_eq!(res.status(), 200);

        let body = res.json_body();
        assert!(body.is_ok());

        let body = body.unwrap();

        let instance_metadata = aws.instances.get_key_value(&job_id).unwrap().1;

        assert_eq!(body.get("ip").unwrap(), &instance_metadata.ip_address);
        assert_eq!(
            body.get("enclave_state").unwrap(),
            &instance_metadata.enclave_metadata.state
        );
        let enclave_eif_prc = body.get("enclave_eif_prc").unwrap();
        assert_eq!(
            enclave_eif_prc.get("prc0").unwrap(),
            &instance_metadata.enclave_metadata.prc.prc0
        );
        assert_eq!(
            enclave_eif_prc.get("prc1").unwrap(),
            &instance_metadata.enclave_metadata.prc.prc1
        );
        assert_eq!(
            enclave_eif_prc.get("prc2").unwrap(),
            &instance_metadata.enclave_metadata.prc.prc2
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_get_ip_bad_case() -> anyhow::Result<()> {
        let mut aws: TestAws = Default::default();

        for id in 1..4 {
            let temp_job_id = H256::from_low_u64_be(id).encode_hex();
            let instance_metadata = InstanceMetadata::new(None, None, None).await;

            aws.instances
                .insert(temp_job_id.clone(), instance_metadata.clone());
        }

        let regions: Vec<String> = vec![String::from("ap-south-1")];
        let compute_rates: &'static [RegionalRates] = Box::leak(vec![].into_boxed_slice());
        let bandwidth_rates: &'static [GBRateCard] = Box::leak(vec![].into_boxed_slice());
        let port = Some(8082);

        tokio::spawn(serve(aws, regions, compute_rates, bandwidth_rates, port));

        let hc = httpc_test::new_client(format!("http://localhost:{}", port.unwrap()))?;

        let job_id = H256::from_low_u64_be(5).encode_hex();

        let res = hc
            .do_get(&format!("/ip?id={}&region=ap-south-1", job_id))
            .await?;
        assert_eq!(res.status(), 400);

        let body = res.json_body();
        assert!(body.is_err());

        let body = res.text_body();
        assert!(body.is_ok());

        let err_message = body.unwrap();
        assert_eq!(err_message, "BAD_REQUEST");

        Ok(())
    }

    #[tokio::test]
    async fn test_get_ip_bad_case_no_instances() -> anyhow::Result<()> {
        let aws: TestAws = Default::default();
        let regions: Vec<String> = vec![String::from("ap-south-1")];
        let compute_rates: &'static [RegionalRates] = Box::leak(vec![].into_boxed_slice());
        let bandwidth_rates: &'static [GBRateCard] = Box::leak(vec![].into_boxed_slice());
        let port = Some(8083);

        tokio::spawn(serve(aws, regions, compute_rates, bandwidth_rates, port));

        let hc = httpc_test::new_client(format!("http://localhost:{}", port.unwrap()))?;

        let res = hc.do_get("/spec").await?;
        assert_eq!(res.status(), 200);

        let body = res.json_body();
        assert!(body.is_ok());

        let job_id = H256::from_low_u64_be(1).encode_hex();

        let res = hc
            .do_get(&format!("/ip?id={}&region=ap-south-1", job_id))
            .await?;
        assert_eq!(res.status(), 400);

        let body = res.json_body();
        assert!(body.is_err());

        let body = res.text_body();
        assert!(body.is_ok());

        let err_message = body.unwrap();
        assert_eq!(err_message, "BAD_REQUEST");

        Ok(())
    }

    #[tokio::test]
    async fn test_handle_spec_request() -> anyhow::Result<()> {
        let aws: TestAws = Default::default();
        let regions: Vec<String> = vec![String::from("ap-south-1")];
        let compute_rates: &'static [RegionalRates] = Box::leak(
            vec![RegionalRates {
                region: String::from("ap-south-1"),
                rate_cards: vec![
                    RateCard {
                        instance: String::from("m5a.16xlarge"),
                        min_rate: U256::try_from(810833333333333 as i64)?,
                        cpu: 64,
                        memory: 256,
                        arch: String::from("amd64"),
                    },
                    RateCard {
                        instance: String::from("c6g.16xlarge"),
                        min_rate: U256::try_from(640722222222222 as i64)?,
                        cpu: 64,
                        memory: 128,
                        arch: String::from("arm64"),
                    },
                    RateCard {
                        instance: String::from("c6i.4xlarge"),
                        min_rate: U256::try_from(207777777777777 as i64)?,
                        cpu: 16,
                        memory: 32,
                        arch: String::from("amd64"),
                    },
                ],
            }]
            .into_boxed_slice(),
        );

        let bandwidth_rates: &'static [GBRateCard] = Box::leak(vec![].into_boxed_slice());
        let port = Some(8084);
        tokio::spawn(serve(aws, regions, compute_rates, bandwidth_rates, port));

        let hc = httpc_test::new_client(format!("http://localhost:{}", port.unwrap()))?;

        let res = hc.do_get("/spec").await?;
        assert_eq!(res.status(), 200);

        let body = res.json_body();
        assert!(body.is_ok());

        let body = json!(body.unwrap());
        let body: Vec<RegionalRates> =
            serde_json::from_value(body.get("min_rates").unwrap().clone()).unwrap();

        assert_eq!(body, compute_rates);

        Ok(())
    }

    #[tokio::test]
    async fn test_handle_bandwidth_request() -> anyhow::Result<()> {
        let aws: TestAws = Default::default();
        let regions: Vec<String> = vec![String::from("ap-south-1")];
        let compute_rates: &'static [RegionalRates] = Box::leak(vec![].into_boxed_slice());

        let bandwidth_rates: &'static [GBRateCard] = Box::leak(
            vec![
                GBRateCard {
                    region: String::from("US East (Ohio)"),
                    region_code: String::from("us-east-2"),
                    rate: U256::try_from(90000000000000000 as i64)?,
                },
                GBRateCard {
                    region: String::from("US East (N. Virginia)"),
                    region_code: String::from("us-east-1"),
                    rate: U256::try_from(90000000000000000 as i64)?,
                },
                GBRateCard {
                    region: String::from("US West (N. California)"),
                    region_code: String::from("us-west-1"),
                    rate: U256::try_from(90000000000000000 as i64)?,
                },
            ]
            .into_boxed_slice(),
        );
        let port = Some(8085);
        tokio::spawn(serve(aws, regions, compute_rates, bandwidth_rates, port));

        let hc = httpc_test::new_client(format!("http://localhost:{}", port.unwrap()))?;

        let res = hc.do_get("/bandwidth").await?;
        assert_eq!(res.status(), 200);

        let body = res.json_body();
        assert!(body.is_ok());

        let body = json!(body.unwrap());
        let body: Vec<GBRateCard> =
            serde_json::from_value(body.get("rates").unwrap().clone()).unwrap_or_default();

        assert_eq!(body, bandwidth_rates);

        Ok(())
    }
}
