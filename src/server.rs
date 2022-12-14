use std::net::{TcpStream, TcpListener};
use std::io::{Read, Write};
use crate::launcher;

async fn handle_read(mut stream: &TcpStream) -> String {
    let mut buf = [0u8 ;4096];
    match stream.read(&mut buf) {
        Ok(_) => {
            let req_str = String::from_utf8_lossy(&buf);
            let mut lines = req_str.lines();
            let mut get_req = lines.next().unwrap().split_whitespace();
            let mut body = get_req.nth(1).unwrap().split("/");

            if body.nth(1).unwrap() == "id" {
                let id = body.next().unwrap();
                if body.next().unwrap() == "ip" {
                    let (exist, ip) = get_ip(id.to_string()).await;
                    let res = "{\"id\": \"".to_owned()+ ip.as_str() +"\"}";
                    let len = res.len();
                    if exist {
                        return "HTTP/1.1 200 OK\r\nContent-Type: application/json;\r\nContent-Length: ".to_owned() + &len.to_string() +"\r\n\r\n" + res.as_str();
                    } else {
                        return String::from("HTTP/1.1 404 Not Found\r\n");
                    }
                    
                } else {
                    return String::from("HTTP/1.1 400 Bad Request\r\n");
                }
            } else {
                return String::from("HTTP/1.1 400 Bad Request\r\n");
            }
        },
        Err(e) => {
            println!("Unable to read stream: {}", e);
            return String::from("HTTP/1.1 500 Internal Server Error\r\n");
        },
    }
}

async fn handle_write(mut stream: TcpStream, response: String) {
    match stream.write(response.as_bytes()) {
        Ok(_) => println!("Response sent"),
        Err(e) => println!("Failed sending response: {}", e),
    }
}

async fn get_ip(id: String) -> (bool, String) {
    let (exists, instance) = launcher::get_job_instance(id).await;
    if exists {
        let ip = launcher::get_instance_ip(instance).await;
        return (true, ip);
    } else {
        return (false, String::new());
    }

}

async fn handle_client(stream: TcpStream) {
    let response = handle_read(&stream).await;

    handle_write(stream, response).await;
}

pub async fn serve() {
    let listener = TcpListener::bind("127.0.0.1:8080").unwrap();
    println!("Listening for connections on port {}", 8080);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                handle_client(stream).await;
            }
            Err(e) => {
                println!("Unable to connect: {}", e);
            }
        }
    }
}