mod launcher;
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

    let credentials_provider = aws_config::profile::ProfileFileCredentialsProvider::builder()
        .profile_name(&cli.profile)
        .build();
    let config = aws_config::from_env()
        .credentials_provider(credentials_provider)
        .load()
        .await;
    let client = aws_sdk_ec2::Client::new(&config);

    launcher::key_setup(&client, cli.key_name.clone()).await?;

    let _ = tokio::spawn(server::serve(client.clone()));

    market::JobsService::run(market::RealAws {
        client: client,
        key_name: cli.key_name,
    }, market::RealLogger {}, cli.rpc).await;

    Ok(())
}
