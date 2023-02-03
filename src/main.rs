mod aws;
mod market;
mod server;

use std::error::Error;
use clap::Parser;


#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
/// Control plane for Oyster
struct Cli {
    /// AWS profile
    #[clap(short, long, value_parser)]
    profile: String,

    /// AWS keypair name
    #[clap(short, long, value_parser)]
    key_name: String,

    /// AWS regions
    #[clap(short, long, value_parser)]
    region: Vec<String>,

    /// RPC url
    #[clap(short, long, value_parser)]
    rpc: String,
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    println!("{:?}", cli.region);

    let aws = aws::Aws::new(cli.profile, cli.key_name).await;

    aws.key_setup().await?;

    let _ = tokio::spawn(server::serve(aws.clone()));

    market::JobsService::run(aws, market::RealLogger {}, cli.rpc).await;

    Ok(())
}
