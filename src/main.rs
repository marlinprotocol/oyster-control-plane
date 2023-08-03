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

    let address_whitelist_vec: Vec<String> =
        parse_file(cli.address_whitelist.unwrap_or("".to_owned())).await?;
    let address_blacklist_vec: Vec<String> =
        parse_file(cli.address_blacklist.unwrap_or("".to_owned())).await?;
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
