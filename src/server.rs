use crate::aws::Aws;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

#[derive(Serialize, Deserialize)]
struct Spec {
    allowed_regions: Vec<String>,
    min_rates: Vec<RegionalRates>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RateCard {
    pub instance: String,
    pub min_rate: i64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RegionalRates {
    pub region: String,
    pub rate_cards: Vec<RateCard>,
}

async fn handle_read(
    client: &Aws,
    mut stream: &TcpStream,
    regions: Vec<String>,
    rates_path: String,
) -> String {
    let mut buf = [0u8; 4096];
    match stream.read(&mut buf) {
        Ok(_) => {
            let req_str = String::from_utf8_lossy(&buf);
            let mut lines = req_str.lines();
            let get_req = lines.next();
            if get_req.is_none() {
                return String::from("HTTP/1.1 400 Bad Request\r\n");
            }
            let mut get_req = get_req.unwrap().split_whitespace();
            let body = get_req.nth(1);
            if body.is_none() {
                return String::from("HTTP/1.1 400 Bad Request\r\n");
            }
            let mut body = body.unwrap().split('/');
            let route = body.nth(1);
            if route.is_none() {
                return String::from("HTTP/1.1 400 Bad Request\r\n");
            }
            let route = route.unwrap();
            if route.starts_with("ip") {
                let query_params: Vec<(String, String)>;
                if let Some(i) = route.find('?') {
                    query_params = route[i + 1..]
                        .split('&')
                        .map(|s| {
                            let mut parts = s.split('=');
                            (
                                parts.next().unwrap().to_owned(),
                                parts.next().unwrap().to_owned(),
                            )
                        })
                        .collect();
                } else {
                    return String::from("HTTP/1.1 400 Bad Request\r\n");
                }
                let mut id = String::new();
                let mut region = String::new();
                for (key, value) in query_params {
                    if key == "id" {
                        id = value;
                    } else if key == "region" {
                        region = value;
                    }
                }
                if id.as_str() == "" || region.as_str() == "" {
                    return String::from("HTTP/1.1 400 Bad Request\r\n");
                }
                let ip = get_ip(client, id, region).await;
                if let Err(err) = ip {
                    println!("Server: {}", err);
                    String::from("HTTP/1.1 404 Not Found\r\n")
                } else {
                    let res = "{\"id\": \"".to_owned() + ip.unwrap().as_str() + "\"}";
                    let len = res.len();
                    return "HTTP/1.1 200 OK\r\nContent-Type: application/json;\r\nContent-Length: "
                        .to_owned()
                        + &len.to_string()
                        + "\r\n\r\n" + res.as_str();
                }
            } else if route.starts_with("spec") {
                let file_path = rates_path.clone();
                let contents = fs::read_to_string(file_path);

                if let Err(err) = contents {
                    println!("Server : Error reading rates file : {}", err);
                } else {
                    let contents = contents.unwrap();
                    let data: Vec<RegionalRates> =
                        serde_json::from_str(&contents).unwrap_or_default();
                    if !data.is_empty() {
                        let res = Spec {
                            allowed_regions: regions,
                            min_rates: data,
                        };
                        let res = serde_json::to_string(&res).unwrap();
                        let len = res.len();
                        return "HTTP/1.1 200 OK\r\nContent-Type: application/json;\r\nContent-Length: ".to_owned() + &len.to_string() +"\r\n\r\n" + res.as_str();
                    }
                }
                return String::from("HTTP/1.1 500 Internal Server Error\r\n");
            } else {
                return String::from("HTTP/1.1 400 Bad Request\r\n");
            }
        }
        Err(e) => {
            println!("Server: Unable to read stream: {}", e);
            String::from("HTTP/1.1 500 Internal Server Error\r\n")
        }
    }
}

async fn handle_write(mut stream: TcpStream, response: String) {
    match stream.write(response.as_bytes()) {
        Ok(_) => println!("Server: Response sent"),
        Err(e) => println!("Server: Failed sending response: {}", e),
    }
}

async fn get_ip(client: &Aws, id: String, region: String) -> Result<String> {
    let instance = client.get_job_instance_id(id, region.clone()).await?;

    let ip = client.get_instance_ip(instance, region).await?;

    Ok(ip)
}

async fn handle_client(client: &Aws, stream: TcpStream, regions: Vec<String>, rates_path: String) {
    let response = handle_read(client, &stream, regions, rates_path).await;

    handle_write(stream, response).await;
}

pub async fn serve(client: Aws, regions: Vec<String>, rates_path: String) {
    let listener = TcpListener::bind("127.0.0.1:8080").unwrap();
    println!("Listening for connections on port {}", 8080);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                handle_client(&client, stream, regions.clone(), rates_path.clone()).await;
            }
            Err(e) => {
                println!("Server: Unable to connect: {}", e);
            }
        }
    }
}
