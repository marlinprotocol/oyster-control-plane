mod launcher;
mod market;
mod server;

use crate::market::*;
use std::error::Error;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    launcher::key_setup().await;

    let server = tokio::spawn(server::serve());

    let js = market::JobsService {};
    js.run("wss://arb-goerli.g.alchemy.com/v2/KYCa2H4IoaidJPaStdaPuUlICHYhCWo3".to_string())
        .await;

    Ok(())
}
