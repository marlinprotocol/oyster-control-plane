use aws_config::profile::ProfileFileCredentialsProvider;
use aws_sdk_ec2::types::SdkError;
use aws_sdk_ec2::Client;
use aws_sdk_ec2::Error;
use ssh2::Session;
use std::env;
use std::fs;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::net::TcpStream;
use std::os::linux::fs::MetadataExt;
use std::path::Path;
use std::process;
use std::process::Command;
use tokio::time::{sleep, Duration};

/* AWS KEY PAIR UTILITY */

pub async fn key_setup() {
    let args: Vec<String> = env::args().collect();

    let aws_profile = &args[1];
    let key_pair_name = &args[2];
    let key_location = &args[3];

    let credentials_provider = ProfileFileCredentialsProvider::builder()
        .profile_name(aws_profile)
        .build();

    let config = aws_config::from_env()
        .credentials_provider(credentials_provider)
        .load()
        .await;

    let client = aws_sdk_ec2::Client::new(&config);

    let key_check = check_key_pair(&client, key_pair_name).await;
    let file_check = Path::new(key_location).exists();
    if !file_check {
        println!("key file not found");
    }

    if !(key_check || file_check) {
        let _key = create_key_pair(&client, key_pair_name, key_location).await;
    } else if !(key_check && file_check) {
        process::exit(1);
    }
}

async fn create_key_pair(client: &Client, name: &str, location: &str) -> String {
    let resp = client
        .create_key_pair()
        .key_name(name.to_string())
        .set_key_type(Some(aws_sdk_ec2::model::KeyType::Ed25519))
        .send()
        .await;

    let mut fingerprint = String::new();

    match resp {
        Ok(res) => {
            fingerprint.push_str(res.key_material().unwrap_or_default());
        }
        Err(e) => {
            panic!("Error: {}", e.to_string());
        }
    }

    println!("{}", fingerprint);
    let path = Path::new(location);
    let display = path.display();

    let mut file = match File::create(&path) {
        Err(why) => {
            panic!("couldn't create {}: {}", display, why)
        }
        Ok(file) => file,
    };

    match file.write_all(fingerprint.as_bytes()) {
        Err(why) => panic!("couldn't write to {}: {}", display, why),
        Ok(_) => {
            let mut cmd = Command::new("chmod");
            cmd.arg("400");
            cmd.arg("/home/nisarg/one.pem");
            let _output = cmd.output();

            println!("Key-pair created, private key written to {}", display);
        }
    }
    return "".to_string();
}

async fn check_key_pair(client: &Client, name: &str) -> bool {
    let resp = client
        .describe_key_pairs()
        .key_names(name.to_string())
        .send()
        .await;

    match resp {
        Ok(res) => match res.key_pairs() {
            None => {
                println!("key not found");
                return false;
            }
            Some(_) => {
                return true;
            }
        },
        Err(_) => {
            println!("key not found");
            return false;
        }
    }
}

/* SSH UTILITY */

async fn ssh_connect(ip_address: String, key_location: String) -> Session {
    let tcp = TcpStream::connect(&ip_address).unwrap();
    let mut sess = Session::new().unwrap();
    sess.set_tcp_stream(tcp);
    sess.handshake().unwrap();
    sess.userauth_pubkey_file("ubuntu", None, Path::new(&key_location), None)
        .unwrap();
    assert!(sess.authenticated());
    println!("SSH connection established");
    return sess;
}

async fn run_enclave(sess: &Session) {
    let mut channel = sess.channel_session().unwrap();
    channel
        .exec(
            "nitro-cli run-enclave --cpu-count 2 --memory 4500 --eif-path startup.eif --debug-mode",
        )
        .unwrap();
    let mut s = String::new();
    channel.read_to_string(&mut s).unwrap();
    println!("{}", s);
    let _close = channel.wait_close();
    println!("Enclave running");
}

#[allow(dead_code)]
async fn terminate_enclave(sess: &Session) {
    let mut channel = sess.channel_session().unwrap();
    channel.exec("nitro-cli terminate-enclave --all").unwrap();
    let mut s = String::new();
    channel.read_to_string(&mut s).unwrap();
    println!("{}", s);
    let _close = channel.wait_close();
}

#[allow(dead_code)]
async fn copy_eif(sess: &Session, eif_path: String) -> io::Result<()> {
    let meta = fs::metadata(eif_path.as_str())?;
    let sz = meta.st_size();
    let mut remote_file = sess.scp_send(&Path::new("/home/ubuntu/gg.txt"), 0o777, sz, None)?;
    let data = get_file_as_byte_vec(&eif_path);
    let _written = remote_file.write_all(&data);

    remote_file.send_eof().unwrap();
    remote_file.wait_eof().unwrap();
    remote_file.close().unwrap();
    remote_file.wait_close().unwrap();
    println!("eif file copied");
    Ok(())
}

#[allow(dead_code)]
fn get_file_as_byte_vec(filename: &String) -> Vec<u8> {
    let mut f = File::open(&filename).expect("no file found");
    let metadata = fs::metadata(&filename).expect("unable to read metadata");
    let mut buffer = vec![0; metadata.len() as usize];
    f.read(&mut buffer).expect("buffer overflow");

    buffer
}

/* AWS EC2 UTILITY */

#[allow(dead_code)]
async fn show_state(client: &Client) -> Result<(), Error> {
    let resp = client.describe_instances().send().await;

    match resp {
        Ok(res) => {
            println!("Instances present :");
            for reservation in res.reservations().unwrap_or_default() {
                for instance in reservation.instances().unwrap_or_default() {
                    println!("Instance ID: {}", instance.instance_id().unwrap());
                    println!(
                        "State:       {:?}",
                        instance.state().unwrap().name().unwrap()
                    );
                    println!();
                }
            }
        }
        Err(e) => {
            println!("Error: {}", e.to_string());
        }
    }

    Ok(())
}

async fn get_instance_ip(client: &Client, instance_id: String) -> String {
    let resp = client
        .describe_instances()
        .instance_ids(instance_id.to_string())
        .send()
        .await;

    match resp {
        Ok(res) => {
            for reservation in res.reservations().unwrap_or_default() {
                for instance in reservation.instances().unwrap_or_default() {
                    return instance.public_ip_address().unwrap().to_string();
                }
            }
        }
        Err(e) => {
            panic!("Error: {}", e.to_string());
        }
    }

    return "".to_string();
}

async fn launch_instance(client: &Client, key_pair_name: &str) -> String {
    let instance_type = aws_sdk_ec2::model::InstanceType::C6aXlarge;
    let instance_ami = "ami-04ce962f738443165";
    let enclave_options = aws_sdk_ec2::model::EnclaveOptionsRequest::builder()
        .set_enabled(Some(true))
        .build();
    let ebs = aws_sdk_ec2::model::EbsBlockDevice::builder()
        .volume_size(25)
        .build();
    let block_device_mapping = aws_sdk_ec2::model::BlockDeviceMapping::builder()
        .set_device_name(Some("/dev/sda1".to_string()))
        .set_ebs(Some(ebs))
        .build();
    let name_tag = aws_sdk_ec2::model::Tag::builder()
        .set_key(Some("Name".to_string()))
        .set_value(Some("tester".to_string()))
        .build();
    let managed_tag = aws_sdk_ec2::model::Tag::builder()
        .set_key(Some("managedBy".to_string()))
        .set_value(Some("marlin".to_string()))
        .build();
    let tags = aws_sdk_ec2::model::TagSpecification::builder()
        .set_resource_type(Some(aws_sdk_ec2::model::ResourceType::Instance))
        .tags(name_tag)
        .tags(managed_tag)
        .build();

    let resp = client
        .run_instances()
        .set_image_id(Some(instance_ami.to_string()))
        .set_instance_type(Some(instance_type))
        .set_key_name(Some(key_pair_name.to_string()))
        .set_min_count(Some(1))
        .set_max_count(Some(1))
        .set_enclave_options(Some(enclave_options))
        .block_device_mappings(block_device_mapping)
        .tag_specifications(tags)
        .send()
        .await;

    match resp {
        Ok(res) => {
            for instance in res.instances().unwrap_or_default() {
                println!(
                    "Instance launched - ID: {}",
                    instance.instance_id().unwrap()
                );
                return instance.instance_id().unwrap().to_string();
            }
        }
        Err(e) => {
            panic!("Error: {}", e.to_string());
        }
    }

    return "".to_string();
}

async fn terminate_instance(client: &Client, instance_id: String) -> Result<(), Error> {
    let resp = client
        .terminate_instances()
        .instance_ids(instance_id)
        .send()
        .await;

    match resp {
        Ok(_) => {
            println!("Instance terminated");
        }
        Err(SdkError::ServiceError { err, .. }) => {
            if err.code().unwrap() == "InvalidInstanceID.NotFound" {
                println!("Instance not found")
            }
        }
        Err(e) => {
            panic!("Error: {}", e.to_string());
        }
    }

    Ok(())
}

pub async fn get_job_instance(job: String) -> (bool, String) {
    let args: Vec<String> = env::args().collect();

    let aws_profile = &args[1];
    let key_pair_name = &args[2];
    let key_location = &args[3];

    let credentials_provider = ProfileFileCredentialsProvider::builder()
        .profile_name(aws_profile)
        .build();

    let config = aws_config::from_env()
        .credentials_provider(credentials_provider)
        .load()
        .await;

    let client = aws_sdk_ec2::Client::new(&config);

    let resp = client.describe_instances().send().await;

    match resp {
        Ok(res) => {
            println!("Instances present :");
            for reservation in res.reservations().unwrap_or_default() {
                for instance in reservation.instances().unwrap_or_default() {
                    let instance_id = instance.instance_id().unwrap();
                    let tags = instance.tags().unwrap();

                    for tag in tags {
                        if tag.key().unwrap() == "jobId" && tag.value().unwrap().to_string() == job
                        {
                            return (true, instance_id.to_string());
                        }
                    }
                }
            }
        }
        Err(e) => {
            println!("Error: {}", e.to_string());
        }
    }
    return (false, String::new());
}

pub async fn spin_up() -> String {
    let args: Vec<String> = env::args().collect();

    let aws_profile = &args[1];
    let key_pair_name = &args[2];
    let key_location = &args[3];

    let credentials_provider = ProfileFileCredentialsProvider::builder()
        .profile_name(aws_profile)
        .build();

    let config = aws_config::from_env()
        .credentials_provider(credentials_provider)
        .load()
        .await;

    let client = aws_sdk_ec2::Client::new(&config);

    let instance = launch_instance(&client, key_pair_name).await;
    sleep(Duration::from_secs(100)).await;
    let mut public_ip_address = get_instance_ip(&client, instance.to_string()).await;
    public_ip_address.push_str(":22");
    let sess = ssh_connect(public_ip_address, key_location.to_string()).await;
    run_enclave(&sess).await;
    return instance;
}

pub async fn spin_down(instance_id: String) {
    let args: Vec<String> = env::args().collect();

    let aws_profile = &args[1];

    let credentials_provider = ProfileFileCredentialsProvider::builder()
        .profile_name(aws_profile)
        .build();

    let config = aws_config::from_env()
        .credentials_provider(credentials_provider)
        .load()
        .await;

    let client = aws_sdk_ec2::Client::new(&config);
    let instance_id = String::new();
    let _done = terminate_instance(&client, instance_id).await;
}
