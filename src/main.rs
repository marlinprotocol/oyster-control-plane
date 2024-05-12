use std::fs;
use std::net::SocketAddr;

use anyhow::Context;
use anyhow::Result;
use clap::Parser;
use ethers::abi::AbiEncode;
use ethers::prelude::*;
use ethers::providers::{Provider, Ws};
use tracing::Instrument;
use tracing::{error, info, info_span};
use tracing_subscriber::EnvFilter;

use cp::aws;
use cp::market;
use cp::server;

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

async fn parse_compute_rates_file(filepath: String) -> Result<Vec<market::RegionalRates>> {
    if filepath.is_empty() {
        return Ok(Vec::new());
    }

    let contents = fs::read_to_string(filepath).context("Error reading file")?;
    let rates: Vec<market::RegionalRates> =
        serde_json::from_str(&contents).context("failed to parse rates file")?;

    Ok(rates)
}

async fn parse_bandwidth_rates_file(filepath: String) -> Result<Vec<market::GBRateCard>> {
    if filepath.is_empty() {
        return Ok(Vec::new());
    }

    let contents = fs::read_to_string(filepath).context("Error reading file")?;
    let rates: Vec<market::GBRateCard> =
        serde_json::from_str(&contents).context("failed to parse rates file")?;

    Ok(rates)
}

async fn get_chain_id_from_rpc_url(url: String) -> Result<String> {
    let provider = Provider::<Ws>::connect(url)
        .await
        .context("Failed to connect to rpc to fetch chain_id")?;
    let hex_chain_id: String = provider
        .request("eth_chainId", ())
        .await
        .context("Failed to fetch chain_id")?;
    let chain_id =
        u64::from_str_radix(&hex_chain_id[2..], 16).context("Failed to convert chain_id to u64")?;

    Ok(chain_id.to_string())
}

async fn run() -> Result<()> {
    let cli = Cli::parse();

    info!(?cli.profile);
    info!(?cli.key_name);
    info!(?cli.rpc);
    info!(?cli.rates);
    info!(?cli.bandwidth);
    info!(?cli.contract);
    info!(?cli.provider);
    info!(?cli.blacklist);
    info!(?cli.whitelist);
    info!(?cli.address_blacklist);
    info!(?cli.address_whitelist);

    let regions: Vec<String> = cli.regions.split(',').map(|r| (r.into())).collect();

    let aws = aws::Aws::new(
        cli.profile,
        &regions,
        cli.key_name,
        cli.whitelist,
        cli.blacklist,
    )
    .await;

    aws.generate_key_pair()
        .await
        .context("Failed to generate key pair")?;

    for region in regions.clone() {
        aws.key_setup(region.clone())
            .await
            .with_context(|| format!("Failed to setup key pair in {region}"))?;
    }

    let compute_rates = parse_compute_rates_file(cli.rates)
        .await
        .context("failed to parse computes rates file")?;
    let bandwidth_rates = parse_bandwidth_rates_file(cli.bandwidth)
        .await
        .context("failed to parse bandwidth rates file")?;

    let address_whitelist_vec: Vec<String> = parse_file(cli.address_whitelist)
        .await
        .context("Failed to parse address whitelist")?;
    let address_blacklist_vec: Vec<String> = parse_file(cli.address_blacklist)
        .await
        .context("Failed to parse address blacklist")?;

    // leak memory to get static references
    // will be cleaned up once program exits
    // alternative to OnceCell equivalents
    let compute_rates: &'static [market::RegionalRates] =
        Box::leak(compute_rates.into_boxed_slice());
    let bandwidth_rates: &'static [market::GBRateCard] =
        Box::leak(bandwidth_rates.into_boxed_slice());
    let address_whitelist: &'static [String] = Box::leak(address_whitelist_vec.into_boxed_slice());
    let address_blacklist: &'static [String] = Box::leak(address_blacklist_vec.into_boxed_slice());
    let regions: &'static [String] = Box::leak(regions.into_boxed_slice());
    let chain = get_chain_id_from_rpc_url(cli.rpc.clone())
        .await
        .context("Failed to fetch chain_id")?;

    let job_id = market::JobId {
        id: H256::zero().encode_hex(),
        operator: cli.provider.clone(),
        contract: cli.contract.clone(),
        chain,
    };

    tokio::spawn(
        server::serve(
            aws.clone(),
            regions,
            compute_rates,
            bandwidth_rates,
            SocketAddr::from(([0, 0, 0, 0], 8080)),
            job_id.clone(),
        )
        .instrument(info_span!("server")),
    );

    let ethers = market::EthersProvider {
        contract: cli
            .contract
            .parse::<Address>()
            .context("failed to parse contract address")?,
        provider: cli
            .provider
            .parse::<Address>()
            .context("failed to parse provider address")?,
    };

    market::run(
        aws,
        ethers,
        cli.rpc,
        regions,
        compute_rates,
        bandwidth_rates,
        address_whitelist,
        address_blacklist,
        job_id,
    )
    .instrument(info_span!("main"))
    .await;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // seems messy, see if there is a better way
    let mut filter = EnvFilter::new("info,aws_config=warn");
    if let Ok(var) = std::env::var("RUST_LOG") {
        filter = filter.add_directive(var.parse()?);
    }
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_env_filter(filter)
        .init();

    let _ = run().await.inspect_err(|e| error!(?e, "run error"));

    Ok(())
}
