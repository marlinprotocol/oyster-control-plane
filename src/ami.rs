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

    let aws = aws::Aws::new(
        cli.profile,
        &[cli.region.clone()],
        String::new(),
        None,
        None,
    )
    .await;
    println!(
        "amd64 community ami: {}",
        aws.get_community_amis(&cli.region, &cli.family, "amd64")
            .await
            .context("failed to fetch amd64 community ami")?
    );
    println!(
        "arm64 community ami: {}",
        aws.get_community_amis(&cli.region, &cli.family, "arm64")
            .await
            .context("failed to fetch arm64 community ami")?
    );

    println!(
        "amd64 resolved ami: {}",
        aws.get_amis(&cli.region, &cli.family, "amd64")
            .await
            .context("failed to fetch amd64 resolved ami")?
    );
    println!(
        "arm64 resolved ami: {}",
        aws.get_amis(&cli.region, &cli.family, "arm64")
            .await
            .context("failed to fetch arm64 resolved ami")?
    );

    Ok(())
}
