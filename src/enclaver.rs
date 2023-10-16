use cp::aws;

use anyhow::Context;
use anyhow::Result;
use clap::Parser;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
/// Control plane for Oyster
struct Cli {
    /// AWS profile
    #[clap(long, value_parser)]
    profile: String,

    /// Instance id
    #[clap(long, value_parser)]
    instance: String,

    /// AWS region
    #[clap(long, value_parser, default_value = "ap-south-1")]
    region: String,

    /// AMI family
    #[clap(long, value_parser, default_value = "salmon")]
    family: String,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let cli = Cli::parse();

    let aws = aws::Aws::new(cli.profile, String::new(), String::new(), String::new()).await;
    aws.run_enclave_impl(&cli.family, &cli.instance, cli.region, "", 2, 4096, 32)
        .await
        .context("could not deploy enclave")?;

    Ok(())
}
