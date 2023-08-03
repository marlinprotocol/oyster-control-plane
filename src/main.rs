mod aws;
mod market;
mod server;
#[cfg(test)]
mod test;

use anyhow::Context;
use anyhow::Result;
use clap::Parser;
use std::fs;

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
    #[clap(
        long,
        value_parser,
        default_value = "us-east-1,us-east-2,us-west-1,us-west-2,ca-central-1,sa-east-1,eu-north-1,eu-west-3,eu-west-2,eu-west-1,eu-central-1,eu-central-2,eu-south-1,eu-south-2,me-south-1,me-central-1,af-south-1,ap-south-1,ap-south-2,ap-northeast-1,ap-northeast-2,ap-northeast-3,ap-southeast-1,ap-southeast-2,ap-southeast-3,ap-southeast-4,ap-east-1"
    )]
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
    #[clap(long, value_parser, default_value = "")]
    blacklist: String,

    /// Whitelist location
    #[clap(long, value_parser, default_value = "")]
    whitelist: String,

    /// Address Blacklist location
    #[clap(long, value_parser, default_value = "")]
    address_blacklist: String,

    /// Address Whitelist location
    #[clap(long, value_parser, default_value = "")]
    address_whitelist: String,
}

async fn parse_file(filepath: String) -> Result<Vec<String>> {
    if filepath.is_empty() {
        return Ok(Vec::new());
    }

    let contents = fs::read_to_string(filepath).context("Error reading file")?;
    let lines: Vec<String> = contents.lines().map(|s| s.to_string()).collect();

    Ok(lines)
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let cli = Cli::parse();

    let regions: Vec<String> = cli.regions.split(',').map(|r| (r.into())).collect();
    println!("Supported regions: {regions:?}");

    let aws = aws::Aws::new(cli.profile, cli.key_name, cli.whitelist, cli.blacklist).await;

    aws.generate_key_pair()
        .await
        .context("Failed to generate key pair")?;

    for region in regions.clone() {
        aws.key_setup(region)
            .await
            .context("Failed to setup key pair in {region}")?;
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

    let address_whitelist_vec: Vec<String> = parse_file(cli.address_whitelist)
        .await
        .context("Failed to parse address whitelist")?;
    let address_blacklist_vec: Vec<String> = parse_file(cli.address_blacklist)
        .await
        .context("Failed to parse address blacklist")?;
    // Converting Vec<String> to &'static [String]
    // because market::run_once needs a static [String]
    let address_whitelist: &'static [String] = Box::leak(address_whitelist_vec.into_boxed_slice());
    let address_blacklist: &'static [String] = Box::leak(address_blacklist_vec.into_boxed_slice());

    market::run(
        aws,
        ethers,
        cli.rpc,
        regions,
        cli.rates,
        cli.bandwidth,
        address_whitelist,
        address_blacklist,
    )
    .await;

    Ok(())
}
