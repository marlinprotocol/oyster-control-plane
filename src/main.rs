mod launcher;
mod market;
mod server;

use std::error::Error;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    launcher::key_setup().await;

    let _ = tokio::spawn(server::serve());

    // let js = market::JobsService {};
    market::JobsService::run(market::RealAws {}, market::RealLogger {}, "wss://arb-goerli.g.alchemy.com/v2/KYCa2H4IoaidJPaStdaPuUlICHYhCWo3".to_string())
        .await;

    Ok(())
}
