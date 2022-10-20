mod market;

use std::error::Error;
use crate::market::*;

#[tokio::main(flavor = "current_thread")]
pub async fn main() -> Result<(), Box<dyn Error>> {
    let js = market::JobsService{};
    js.run("wss://arb-goerli.g.alchemy.com/v2/KYCa2H4IoaidJPaStdaPuUlICHYhCWo3".to_string()).await;
    Ok(())
}
