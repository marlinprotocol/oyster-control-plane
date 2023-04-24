mod aws;
mod market;
mod server;
mod test;

use clap::Parser;
use std::error::Error;

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
    #[clap(long, value_parser)]
    regions: String,

    /// RPC url
    #[clap(long, value_parser)]
    rpc: String,

    /// Rates location
    #[clap(long, value_parser)]
    rates: String,

    /// Blacklist location
    #[clap(default_value_t=String::new(),long, value_parser)]
    black: String,

    /// Whitelist location
    #[clap(default_value_t=String::new(),long, value_parser)]
    white: String,
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    let regions: Vec<String> = cli.regions.split(',').map(|r| (r.into())).collect();
    println!("Supported regions: {regions:?}");

    let aws = aws::Aws::new(cli.profile, cli.key_name, cli.white, cli.black).await;

    aws.generate_key_pair().await?;

    for region in regions.clone() {
        aws.key_setup(region).await?;
    }

    tokio::spawn(server::serve(
        aws.clone(),
        regions.clone(),
        cli.rates.clone(),
    ));

    market::JobsService::run(aws, market::RealLogger {}, cli.rpc, regions, cli.rates).await;

    Ok(())
}
