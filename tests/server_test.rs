use anyhow::Result;

#[tokio::test]
#[ignore]
async fn test_handle_spec_request() -> Result<()> {
    let hc = httpc_test::new_client("http://localhost:8080")?;

    let res = hc.do_get("/spec").await?;

    assert_eq!(res.status(), 200);

    Ok(())
}
