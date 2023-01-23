use aws_config::profile::ProfileFileCredentialsProvider;
use aws_sdk_ec2::types::SdkError;
use aws_sdk_ec2::Client;
use aws_sdk_ec2::Error;
use ssh2::Session;
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
use std::str::FromStr;
use clap::Parser;

/* AWS KEY PAIR UTILITY */

pub async fn key_setup() {

    let (aws_profile, key_pair_name, key_location) = get_envs();

    let credentials_provider = ProfileFileCredentialsProvider::builder()
        .profile_name(aws_profile.as_str())
        .build();

    let config = aws_config::from_env()
        .credentials_provider(credentials_provider)
        .load()
        .await;

    let client = aws_sdk_ec2::Client::new(&config);

    let key_check = check_key_pair(&client, &key_pair_name).await;
    let file_check = Path::new(key_location.as_str()).exists();
    if !file_check {
        println!("key file not found");
    }

    if !(key_check || file_check) {
        let _key = create_key_pair(&client, &key_pair_name, key_location.as_str()).await;
    } else if !(key_check && file_check) {
        process::exit(1);
    }
}

async fn create_key_pair(client: &Client, name: &String, location: &str) -> String {
    let resp = client
        .create_key_pair()
        .key_name(name)
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

async fn check_key_pair(client: &Client, name: &String) -> bool {
    let resp = client
        .describe_key_pairs()
        .key_names(name)
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

async fn run_enclave(sess: &Session, url: &str, v_cpus: i32, mem: i64) {
    let mut channel = sess.channel_session().unwrap();
    let mut s = String::new();
    channel
        .exec(
            &("echo -e '---\\nmemory_mib: ".to_owned() + &((mem-2048).to_string()) + "\\ncpu_count: " + &((v_cpus-2).to_string()) + "' >> /home/ubuntu/allocator_new.yaml"),
        )
        .unwrap();
    channel.read_to_string(&mut s).unwrap();
    let _ = channel.wait_close();
    println!("{}", s);

    channel = sess.channel_session().unwrap();
    channel
        .exec(
            &("sudo cp /home/ubuntu/allocator_new.yaml /etc/nitro_enclaves/allocator.yaml"),
        )
        .unwrap();

    channel.read_to_string(&mut s).unwrap();
    println!("{}", s);
    let _ = channel.wait_close();

    channel = sess.channel_session().unwrap();
    channel
        .exec(
            &("sudo systemctl restart nitro-enclaves-allocator.service"),
        )
        .unwrap();

    channel.read_to_string(&mut s).unwrap();
    println!("{}", s);
    let _ = channel.wait_close();
    
    println!("Nitro Enclave Service set up with cpus: {} and memory: {}", v_cpus-2, mem-2048);

    channel = sess.channel_session().unwrap();
    channel
        .exec(
            &("wget -O enclave.eif ".to_owned() + url),
        )
        .unwrap();
    channel.read_to_string(&mut s).unwrap();
    let _ = channel.wait_close();
    println!("{}", s);

    channel = sess.channel_session().unwrap();
    channel
        .exec(
            &("sudo iptables -A PREROUTING -t nat -p tcp --dport 80 -i ens5 -j REDIRECT --to-port 1200"),
        )
        .unwrap();

    channel.read_to_string(&mut s).unwrap();
    println!("{}", s);
    let _ = channel.wait_close();

    channel = sess.channel_session().unwrap();
    channel
        .exec(
            &("sudo iptables -A PREROUTING -t nat -p tcp --dport 443 -i ens5 -j REDIRECT --to-port 1200"),
        )
        .unwrap();

    channel.read_to_string(&mut s).unwrap();
    println!("{}", s);
    let _ = channel.wait_close();

    channel = sess.channel_session().unwrap();
    channel
        .exec(
            &("sudo iptables -A PREROUTING -t nat -p tcp --dport 1025:65535 -i ens5 -j REDIRECT --to-port 1200"),
        )
        .unwrap();

    channel.read_to_string(&mut s).unwrap();
    println!("{}", s);
    let _ = channel.wait_close();

    channel = sess.channel_session().unwrap();
    channel
        .exec(
            &("nitro-cli run-enclave --cpu-count ".to_owned() + &((v_cpus-2).to_string()) + " --memory " + &((mem-2200).to_string()) +" --eif-path enclave.eif --enclave-cid 88"),
        )
        .unwrap();

    channel.read_to_string(&mut s).unwrap();
    println!("{}", s);
    let _ = channel.wait_close();

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

pub async fn get_instance_ip(instance_id: String) -> String {
    let (aws_profile, _, _) = get_envs();

    let credentials_provider = ProfileFileCredentialsProvider::builder()
        .profile_name(aws_profile.as_str())
        .build();

    let config = aws_config::from_env()
        .credentials_provider(credentials_provider)
        .load()
        .await;

    let client = aws_sdk_ec2::Client::new(&config);

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

pub async fn launch_instance(client: &Client, key_pair_name: String, job: String, instance_type: &str, image_url: &str, architecture: String) -> String {
    
    let req_client = reqwest::Client::builder()
            .no_gzip()
            .build()
            .unwrap();
    let res = req_client.head(image_url).send().await;
    let res = res.unwrap();
    let size = res.headers()["content-length"].to_str().unwrap();
    

    let size = size.parse::<i64>().unwrap() / 1000000;
    println!("eif size: {} MB", size);
    let size = size / 1000;
    let mut sdd = 15;
    if size > sdd {
        sdd = size + 10;
    }
    
    let instance_type = aws_sdk_ec2::model::InstanceType::from_str(instance_type).unwrap();
    let (x86_ami, arm_ami) = get_amis(&client).await; 
    if x86_ami == String::new() || arm_ami == String::new() {
        panic!("AMI's not found");
    }
    let mut instance_ami = x86_ami;
    if architecture == "arm64".to_string() {
        instance_ami = arm_ami;
    }
    let enclave_options = aws_sdk_ec2::model::EnclaveOptionsRequest::builder()
        .set_enabled(Some(true))
        .build();
    let ebs = aws_sdk_ec2::model::EbsBlockDevice::builder()
        .volume_size(sdd as i32)
        .build();
    let block_device_mapping = aws_sdk_ec2::model::BlockDeviceMapping::builder()
        .set_device_name(Some("/dev/sda1".to_string()))
        .set_ebs(Some(ebs))
        .build();
    let name_tag = aws_sdk_ec2::model::Tag::builder()
        .set_key(Some("Name".to_string()))
        .set_value(Some("JobRunner".to_string()))
        .build();
    let managed_tag = aws_sdk_ec2::model::Tag::builder()
        .set_key(Some("managedBy".to_string()))
        .set_value(Some("marlin".to_string()))
        .build();
    let project_tag = aws_sdk_ec2::model::Tag::builder()
        .set_key(Some("project".to_string()))
        .set_value(Some("oyster".to_string()))
        .build();
    let job_tag = aws_sdk_ec2::model::Tag::builder()
        .set_key(Some("jobId".to_string()))
        .set_value(Some(job))
        .build();
    let tags = aws_sdk_ec2::model::TagSpecification::builder()
        .set_resource_type(Some(aws_sdk_ec2::model::ResourceType::Instance))
        .tags(name_tag)
        .tags(managed_tag)
        .tags(job_tag)
        .tags(project_tag)
        .build();
    let subnet = get_subnet().await;
    let sec_group = get_security_group().await;


    let resp = client
        .run_instances()
        .set_image_id(Some(instance_ami))
        .set_instance_type(Some(instance_type))
        .set_key_name(Some(key_pair_name))
        .set_min_count(Some(1))
        .set_max_count(Some(1))
        .set_enclave_options(Some(enclave_options))
        .block_device_mappings(block_device_mapping)
        .tag_specifications(tags)
        .security_group_ids(sec_group)
        .subnet_id(subnet)
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

async fn terminate_instance(client: &Client, instance_id: &String) -> Result<(), Error> {
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

async fn get_amis(client: &Client) -> (String, String) {
    let mut arm_ami = String::new();
    let mut x86_ami = String::new();

    let filter = aws_sdk_ec2::model::Filter::builder()
        .name("tag:project")
        .values("oyster")
        .build();

    let resp = client
            .describe_images()
            .owners("self")
            .filters(filter)
            .send()
            .await;

    match resp {
        Ok(res) => {
            for image in res.images().unwrap_or_default() {
                if "MarlinLauncherx86_64" == image.name().unwrap() {
                    println!("x86_64 ami: {}", image.image_id().unwrap());
                    x86_ami = image.image_id().unwrap().to_string();
                } else if "MarlinLauncherARM64" == image.name().unwrap() {
                    println!("arm64 ami: {}", image.image_id().unwrap());
                    arm_ami = image.image_id().unwrap().to_string();
                }
            }
        }
        Err(e) => {
            panic!("Error: {}", e.to_string());
        }
    }
    return (x86_ami, arm_ami);
}

pub async fn get_security_group() -> (String) {
    let mut sec_group = String::new();
    let (aws_profile, _, _) = get_envs();

    let credentials_provider = ProfileFileCredentialsProvider::builder()
        .profile_name(aws_profile.as_str())
        .build();

    let config = aws_config::from_env()
        .credentials_provider(credentials_provider)
        .load()
        .await;

    let client = aws_sdk_ec2::Client::new(&config);

    let filter = aws_sdk_ec2::model::Filter::builder()
        .name("tag:project")
        .values("oyster")
        .build();

    let resp = client
    .describe_security_groups()
    .filters(filter)
    .send()
    .await;

    match resp {
        Ok(res) => {
            for group in res.security_groups().unwrap_or_default() {
                for tagpair in  group.tags().unwrap_or_default() {
                    if "project" == tagpair.key().unwrap() && "oyster" == tagpair.value().unwrap() {
                        
                        return group.group_id().unwrap().to_string()
                    }
                }
            }
        }
        Err(e) => {
            panic!("Error: {}", e.to_string());
        }
    }
    sec_group
} 

pub async fn get_subnet() -> (String) {
    let mut subnet = String::new();
    let (aws_profile, _, _) = get_envs();

    let credentials_provider = ProfileFileCredentialsProvider::builder()
        .profile_name(aws_profile.as_str())
        .build();

    let config = aws_config::from_env()
        .credentials_provider(credentials_provider)
        .load()
        .await;

    let client = aws_sdk_ec2::Client::new(&config);

    let filter = aws_sdk_ec2::model::Filter::builder()
        .name("tag:project")
        .values("oyster")
        .build();

    let resp = client
    .describe_subnets()
    .filters(filter)
    .send()
    .await;

    match resp {
        Ok(res) => {
            for subnet in res.subnets().unwrap_or_default() {
                for tagpair in  subnet.tags().unwrap_or_default() {
                    if "project" == tagpair.key().unwrap() && "oyster" == tagpair.value().unwrap() {
                        println!("{}", subnet.subnet_id().unwrap());
                        return  subnet.subnet_id().unwrap().to_string();
                    }
                }
            }
        }
        Err(e) => {
            panic!("Error: {}", e.to_string());
        }
    }
    subnet
} 

pub async fn get_job_instance(job: String) -> (bool, String) {
    let (aws_profile, _, _) = get_envs();

    let credentials_provider = ProfileFileCredentialsProvider::builder()
        .profile_name(aws_profile.as_str())
        .build();

    let config = aws_config::from_env()
        .credentials_provider(credentials_provider)
        .load()
        .await;

    let client = aws_sdk_ec2::Client::new(&config);

    let resp = client.describe_instances().send().await;

    match resp {
        Ok(res) => {
            println!("Checking existing instance...");
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

fn get_envs() -> (String, String, String) {
    #[derive(Parser)]
    #[clap(about)]
    /// Control plane for enclaves support with the oyster update
    struct Cli {
        /// AWS profile 
        #[clap(short, long, value_parser)]
        profile: String,
        
        /// AWS keypair name 
        #[clap(short, long, value_parser)]
        key_name: String,

        /// Keypair private key file location
        #[clap(short, long, value_parser)]
        loc: String,
    }
    let cli = Cli::parse();

    return (cli.profile, cli.key_name, cli.loc);
}

pub async fn spin_up(image_url: &str, job: String, instance_type: &str) -> String {
    let (aws_profile, key_pair_name, key_location) = get_envs();
    

    let credentials_provider = ProfileFileCredentialsProvider::builder()
        .profile_name(aws_profile.as_str())
        .build();

    let config = aws_config::from_env()
        .credentials_provider(credentials_provider)
        .load()
        .await;

    let client = aws_sdk_ec2::Client::new(&config);
    let resp = client
            .describe_instance_types()
            .instance_types(aws_sdk_ec2::model::InstanceType::from_str(instance_type).unwrap())
            .send()
            .await;
    let mut architecture = "x86_64".to_string();
    let mut v_cpus: i32 = 4;
    let mut mem: i64 = 8192;
    match resp {
        Ok(resp) => {
            for instance in resp.instance_types().unwrap() {
                for arch in instance.processor_info().unwrap().supported_architectures().unwrap() {
                    architecture = arch.as_str().to_string();
                    println!("architecture: {}", arch.as_str());
                    break;
                }
                v_cpus = instance.v_cpu_info().unwrap().default_v_cpus().unwrap();
                println!("v_cpus: {}", instance.v_cpu_info().unwrap().default_v_cpus().unwrap());
                mem = instance.memory_info().unwrap().size_in_mi_b().unwrap();
                println!("memory: {}", instance.memory_info().unwrap().size_in_mi_b().unwrap());
            }  
            
        }   
        Err(e) => {
            println!("Error: {}", e.to_string());
        }
    }

    let instance = launch_instance(&client, key_pair_name, job, instance_type, image_url, architecture).await;
    // sleep(Duration::from_secs(100)).await;
    
    // let mut public_ip_address = get_instance_ip(instance.to_string()).await;
    // public_ip_address.push_str(":22");
    // let sess = ssh_connect(public_ip_address, key_location).await;
    // run_enclave(&sess, image_url, v_cpus, mem).await;
    return instance;
}

pub async fn spin_down(instance_id: &String) {
    let (aws_profile, _, _) = get_envs();

    let credentials_provider = ProfileFileCredentialsProvider::builder()
        .profile_name(aws_profile.as_str())
        .build();

    let config = aws_config::from_env()
        .credentials_provider(credentials_provider)
        .load()
        .await;

    let client = aws_sdk_ec2::Client::new(&config);
    let _done = terminate_instance(&client, &instance_id).await;
}
