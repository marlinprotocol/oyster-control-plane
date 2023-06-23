mod aws;
mod market;
mod server;
mod test;

use clap::Parser;
use std::error::Error;
use std::sync::Arc;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
/// Control plane for Oyster
struct Cli {
    /// AWS profile
    #[clap(long, value_parser)]
    profile: String,

    /// AWS keypair name
    #[clap(long, value_parser)]
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

    /// Bandwidth Rates location
    #[clap(long, value_parser)]
    bandwidth: String,

    /// Contract address
    #[clap(long, value_parser)]
    contract: String,

    /// Provider address
    #[clap(long, value_parser)]
    provider: String,

    /// Blacklist location
    #[clap(long, value_parser)]
    blacklist: Option<String>,

    /// Whitelist location
    #[clap(long, value_parser)]
    whitelist: Option<String>,

    /// Address Blacklist location
    #[clap(long, value_parser)]
    address_blacklist: Option<String>,

    /// Address Whitelist location
    #[clap(long, value_parser)]
    address_whitelist: Option<String>,
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    let regions: Vec<String> = cli.regions.split(',').map(|r| (r.into())).collect();
    println!("Supported regions: {regions:?}");

    let aws = aws::Aws::new(
        cli.profile,
        cli.key_name,
        cli.whitelist.unwrap_or("".to_owned()),
        cli.blacklist.unwrap_or("".to_owned()),
    )
    .await;

    aws.generate_key_pair().await?;

    for region in regions.clone() {
        aws.key_setup(region).await?;
    }

    tokio::spawn(server::serve(
        aws.clone(),
        regions.clone(),
        cli.rates.clone(),
    ));

    let ethers = market::EthersProvider {
        address: cli.contract,
        provider: cli.provider,
    };

    let address_whitelist: Arc<String> =
        match market::parse_file(cli.address_whitelist.unwrap_or("".to_owned())).await {
            Ok(contents) => Arc::new(contents),
            Err(err) => {
                println!("Error parsing whitelist file: {}", err);
                return Err(Box::new(err).context("Failed to parse whitelist file"));
            }
        };
    let address_blacklist: Arc<String> =
        match market::parse_file(cli.address_blacklist.unwrap_or("".to_owned())).await {
            Ok(contents) => Arc::new(contents),
            Err(err) => {
                println!("Error parsing blacklist file: {}", err);
                return Err(Box::new(err).context("Failed to parse blacklist file"));
            }
        };

    market::run(
        aws,
        ethers,
        cli.rpc,
        regions,
        cli.rates,
        cli.bandwidth,
        &address_whitelist,
        &address_blacklist,
    )
    .await;

    Ok(())
}
