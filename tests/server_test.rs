use main::market::GBRateCard;

use anyhow::Result;
use serde_json::json;
use std::fs;

#[tokio::test]
#[ignore]
async fn test_handle_spec_request() -> Result<()> {
    let hc = httpc_test::new_client("http://localhost:8080")?;

    let res = hc.do_get("/spec").await?;
    assert_eq!(res.status(), 200);

    let contents = fs::read_to_string("rates.json");
    assert!(contents.is_ok());

    let contents = contents.unwrap();
    let data: Vec<GBRateCard> = serde_json::from_str(&contents).unwrap_or_default();

    let body = res.json_body();
    assert!(body.is_ok());

    let body = json!(body.unwrap());
    let body: Vec<GBRateCard> =
        serde_json::from_value(body.get("min_rates").unwrap().clone()).unwrap_or_default();

    assert_eq!(body, data);

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_handle_bandwidth_request() -> Result<()> {
    let hc = httpc_test::new_client("http://localhost:8080")?;

    let res = hc.do_get("/bandwidth").await?;
    assert_eq!(res.status(), 200);

    let contents = fs::read_to_string("GB_rates.json");
    assert!(contents.is_ok());

    let contents = contents.unwrap();
    let data: Vec<GBRateCard> = serde_json::from_str(&contents).unwrap_or_default();

    let body = res.json_body();
    assert!(body.is_ok());

    let body = json!(body.unwrap());
    let body: Vec<GBRateCard> =
        serde_json::from_value(body.get("rates").unwrap().clone()).unwrap_or_default();

    assert_eq!(body, data);

    Ok(())
}
