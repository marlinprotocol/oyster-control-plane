use std::error::Error;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    println!("Hello, world!");
    Ok(())
}
