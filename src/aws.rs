use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use aws_types::region::Region;
use rand_core::OsRng;
use serde::Serialize;
use serde_json::Value;
use ssh2::Session;
use ssh_key::{Algorithm, LineEnding, PrivateKey};
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::{BufReader, Read};
use std::net::TcpStream;
use std::path::Path;
use std::str::FromStr;
use tokio::time::{sleep, Duration};
use whoami::username;

use crate::market::InfraProvider;

#[derive(Clone, Debug, Serialize)]
pub struct PRC {
    pub prc0: String,
    pub prc1: String,
    pub prc2: String,
}

#[derive(Clone)]
pub struct Aws {
    aws_profile: String,
    key_name: String,
    // Path cannot be cloned, hence String
    key_location: String,
    pub_key_location: String,
    whitelist: String,
    blacklist: String,
}

impl Aws {
    pub async fn new(
        aws_profile: String,
        key_name: String,
        whitelist: String,
        blacklist: String,
    ) -> Aws {
        let key_location = "/home/".to_owned() + &username() + "/.ssh/" + &key_name + ".pem";
        let pub_key_location = "/home/".to_owned() + &username() + "/.ssh/" + &key_name + ".pub";

        Aws {
            aws_profile,
            key_name,
            key_location,
            pub_key_location,
            whitelist,
            blacklist,
        }
    }

    async fn client(&self, region: String) -> aws_sdk_ec2::Client {
        let config = aws_config::from_env()
            .profile_name(&self.aws_profile)
            .region(Region::new(region))
            .load()
            .await;
        aws_sdk_ec2::Client::new(&config)
    }

    pub async fn generate_key_pair(&self) -> Result<()> {
        let priv_check = Path::new(&self.key_location).exists();
        let pub_check = Path::new(&self.pub_key_location).exists();

        if priv_check && pub_check {
            // both exist, we are done
            Ok(())
        } else if priv_check {
            // only private key exists, generate public key
            let private_key = PrivateKey::read_openssh_file(Path::new(&self.key_location))
                .context("Failed to read private key file")?;

            private_key
                .public_key()
                .write_openssh_file(Path::new(&self.pub_key_location))
                .context("Failed to write public key file")?;

            Ok(())
        } else if pub_check {
            // only public key exists, error out to avoid overwriting it
            Err(anyhow!("Found public key file without corresponding private key file, exiting to prevent overwriting it"))
        } else {
            // neither exist, generate private key and public key
            let private_key = PrivateKey::random(OsRng, Algorithm::Ed25519)
                .context("Failed to generate private key")?;

            private_key
                .write_openssh_file(Path::new(&self.key_location), LineEnding::default())
                .context("Failed to write private key file")?;

            private_key
                .public_key()
                .write_openssh_file(Path::new(&self.pub_key_location))
                .context("Failed to write public key file")?;

            Ok(())
        }
    }

    pub async fn key_setup(&self, region: String) -> Result<()> {
        let key_check = self
            .check_key_pair(region.clone())
            .await
            .context("failed to check key pair")?;

        if !key_check {
            self.import_key_pair(region)
                .await
                .context("Failed to import key pair in {region}")?;
        } else {
            println!("found existing keypair and pem file in {region}, skipping key setup");
        }

        Ok(())
    }

    pub async fn import_key_pair(&self, region: String) -> Result<()> {
        let f = File::open(&self.pub_key_location).context("Failed to open pub key file")?;
        let mut reader = BufReader::new(f);
        let mut buffer = Vec::new();

        reader
            .read_to_end(&mut buffer)
            .context("Failed to read pub key file")?;

        self.client(region)
            .await
            .import_key_pair()
            .key_name(&self.key_name)
            .public_key_material(aws_sdk_ec2::types::Blob::new(buffer))
            .send()
            .await
            .context("Failed to import key pair")?;

        Ok(())
    }

    async fn check_key_pair(&self, region: String) -> Result<bool> {
        Ok(!self
            .client(region)
            .await
            .describe_key_pairs()
            .filters(
                aws_sdk_ec2::model::Filter::builder()
                    .name("key-name")
                    .values(&self.key_name)
                    .build(),
            )
            .send()
            .await
            .context("failed to query key pairs")?
            .key_pairs()
            .ok_or(anyhow!("failed to parse key pairs"))?
            .is_empty())
    }

    /* SSH UTILITY */

    pub async fn ssh_connect(&self, ip_address: &str) -> Result<Session> {
        let tcp = TcpStream::connect(ip_address)?;

        let mut sess = Session::new()?;

        sess.set_tcp_stream(tcp);
        sess.handshake()?;
        sess.userauth_pubkey_file("ubuntu", None, Path::new(&self.key_location), None)?;
        println!("SSH connection established");
        Ok(sess)
    }

    fn ssh_exec(sess: &Session, command: &str) -> Result<(String, String)> {
        let mut channel = sess
            .channel_session()
            .context("Failed to get channel session")?;
        let mut stdout = String::new();
        let mut stderr = String::new();
        channel
            .exec(command)
            .context("Failed to execute command: {command}")?;
        channel
            .read_to_string(&mut stdout)
            .context("Failed to read stdout")?;
        channel
            .stderr()
            .read_to_string(&mut stderr)
            .context("Failed to read stderr")?;
        channel.wait_close().context("Failed to wait for close")?;

        Ok((stdout, stderr))
    }

    pub async fn run_enclave_impl(
        &self,
        instance_id: &str,
        region: String,
        image_url: &str,
        req_vcpu: i32,
        req_mem: i64,
        bandwidth: u64,
    ) -> Result<()> {
        let public_ip_address = self
            .get_instance_ip(instance_id, region.clone())
            .await
            .context("could not fetch instance ip")?;
        let sess = &self
            .ssh_connect(&(public_ip_address + ":22"))
            .await
            .context("error establishing ssh connection")?;

        Self::ssh_exec(
            sess,
            &("echo -e '---\\nmemory_mib: ".to_owned()
                + &((req_mem).to_string())
                + "\\ncpu_count: "
                + &((req_vcpu).to_string())
                + "' > /home/ubuntu/allocator_new.yaml"),
        )
        .context("Failed to set allocator file")?;

        Self::ssh_exec(
            sess,
            "sudo cp /home/ubuntu/allocator_new.yaml /etc/nitro_enclaves/allocator.yaml",
        )
        .context("Failed to copy allocator file")?;

        let (_, stderr) = Self::ssh_exec(
            sess,
            "sudo systemctl restart nitro-enclaves-allocator.service",
        )
        .context("Failed to restart allocator service")?;
        if !stderr.is_empty() {
            println!("{stderr}");
            return Err(anyhow!(
                "Error restarting nitro-enclaves-allocator service: {stderr}"
            ));
        }

        println!("Nitro Enclave Service set up with cpus: {req_vcpu} and memory: {req_mem}");

        Self::ssh_exec(sess, &("wget -O enclave.eif ".to_owned() + image_url))
            .context("Failed to download enclave image")?;

        if self.whitelist.as_str() != "" || self.blacklist.as_str() != "" {
            let (stdout, stderr) = Self::ssh_exec(sess, "sha256sum /home/ubuntu/enclave.eif")
                .context("Failed to calculate image hash")?;
            if !stderr.is_empty() {
                println!("{stderr}");
                return Err(anyhow!("Error calculating hash of enclave image: {stderr}"));
            }

            let line = stdout
                .split_whitespace()
                .next()
                .ok_or(anyhow!("Failed to retrieve image hash: {stdout}"))?;

            println!("Hash: {line}");

            if self.whitelist.as_str() != "" {
                println!("Checking whitelist...");
                let file_path = self.whitelist.as_str();
                let contents =
                    fs::read_to_string(file_path).context("Error reading whitelist file")?;

                let entries = contents.lines();
                let mut allowed = false;
                for entry in entries {
                    if entry.contains(line) {
                        allowed = true;
                        break;
                    }
                }
                if allowed {
                    println!("EIF ALLOWED!");
                } else {
                    println!("EIF NOT ALLOWED!");
                    return Err(anyhow!("EIF NOT ALLOWED"));
                }
            }

            if self.blacklist.as_str() != "" {
                println!("Checking blacklist...");
                let file_path = self.blacklist.as_str();
                let contents =
                    fs::read_to_string(file_path).context("Error reading blacklist file")?;

                let entries = contents.lines();
                let mut allowed = true;
                for entry in entries {
                    if entry.contains(line) {
                        allowed = false;
                        break;
                    }
                }
                if allowed {
                    println!("EIF ALLOWED!");
                } else {
                    println!("EIF NOT ALLOWED!");
                    return Err(anyhow!("EIF NOT ALLOWED"));
                }
            }
        }

        let (stdout, stderr) =
            Self::ssh_exec(sess, "nmcli device status").context("Failed to get nmcli status")?;
        if !stderr.is_empty() || stdout.is_empty() {
            println!("{stderr}");
            return Err(anyhow!("Error fetching network interface name: {stderr}"));
        }
        let mut interface = String::new();
        let entries: Vec<&str> = stdout.split('\n').collect();
        for line in entries {
            let entry: Vec<&str> = line.split_whitespace().collect();
            if entry.len() > 1 && entry[1] == "ethernet" {
                interface = entry[0].to_string();
                break;
            }
        }

        if !interface.is_empty() {
            let (stdout, stderr) = Self::ssh_exec(
                sess,
                &("sudo tc qdisc show dev ".to_owned() + &interface + " root"),
            )
            .context("Failed to fetch tc config")?;
            if !stderr.is_empty() || stdout.is_empty() {
                println!("{stderr}");
                return Err(anyhow!(
                    "Error fetching network interface qdisc configuration: {stderr}"
                ));
            }
            let entries: Vec<&str> = stdout.trim().split('\n').collect();
            let mut is_any_rule_set = true;
            if entries[0].to_lowercase().contains(&"qdisc mq 0: root") && entries.len() == 1 {
                is_any_rule_set = false;
            }

            // remove previously defined rules
            if is_any_rule_set {
                let (_, stderr) = Self::ssh_exec(
                    sess,
                    &("sudo tc qdisc del dev ".to_owned() + &interface + " root"),
                )?;
                if !stderr.is_empty() {
                    println!("{stderr}");
                    return Err(anyhow!(
                        "Error removing network interface qdisc configuration: {stderr}"
                    ));
                }
            }

            let (_, stderr) = Self::ssh_exec(
                sess,
                &("sudo tc qdisc add dev ".to_owned()
                    + &interface
                    + " root tbf rate "
                    + &bandwidth.to_string()
                    + "kbit burst 4000Mb latency 100ms"),
            )?;

            if !stderr.is_empty() {
                println!("{stderr}");
                return Err(anyhow!("Error setting up bandwidth limit: {stderr}"));
            }
        } else {
            return Err(anyhow!("Error fetching network interface name"));
        }

        let iptables_rules: [&str; 4] = [
            "-P PREROUTING ACCEPT",
            "-A PREROUTING -i ens5 -p tcp -m tcp --dport 80 -j REDIRECT --to-ports 1200",
            "-A PREROUTING -i ens5 -p tcp -m tcp --dport 443 -j REDIRECT --to-ports 1200",
            "-A PREROUTING -i ens5 -p tcp -m tcp --dport 1025:65535 -j REDIRECT --to-ports 1200",
        ];
        let (stdout, stderr) = Self::ssh_exec(sess, "sudo iptables -t nat -S PREROUTING")
            .context("Failed to query iptables")?;

        if !stderr.is_empty() || stdout.is_empty() {
            println!("{stderr}");
            return Err(anyhow!("Failed to get iptables rules: {stderr}"));
        }

        let rules: Vec<&str> = stdout.trim().split('\n').map(|s| s.trim()).collect();

        if rules[0] != iptables_rules[0] {
            println!("Got '{}' instead of '{}'", rules[0], iptables_rules[0]);
            return Err(anyhow!("Failed to get PREROUTING ACCEPT rules"));
        }

        if !rules.contains(&iptables_rules[1]) {
            let (_, stderr) = Self::ssh_exec(sess, "sudo iptables -A PREROUTING -t nat -p tcp --dport 80 -i ens5 -j REDIRECT --to-port 1200").context("Failed to set iptables rule")?;
            if !stderr.is_empty() {
                println!("{stderr}");
                return Err(anyhow!("Failed to set iptables rule: {stderr}"));
            }
        }

        if !rules.contains(&iptables_rules[2]) {
            let (_, stderr) = Self::ssh_exec(sess, "sudo iptables -A PREROUTING -t nat -p tcp --dport 443 -i ens5 -j REDIRECT --to-port 1200").context("Failed to set iptables rule")?;
            if !stderr.is_empty() {
                println!("{stderr}");
                return Err(anyhow!("Failed to set iptables rule: {stderr}"));
            }
        }

        if !rules.contains(&iptables_rules[3]) {
            let (_, stderr) = Self::ssh_exec(sess, "sudo iptables -A PREROUTING -t nat -p tcp --dport 1025:65535 -i ens5 -j REDIRECT --to-port 1200").context("Failed to set iptables rule")?;
            if !stderr.is_empty() {
                println!("{stderr}");
                return Err(anyhow!("Failed to set iptables rule: {stderr}"));
            }
        }

        let (_, stderr) = Self::ssh_exec(
            sess,
            &("nitro-cli run-enclave --cpu-count ".to_owned()
                + &((req_vcpu).to_string())
                + " --memory "
                + &((req_mem).to_string())
                + " --eif-path enclave.eif --enclave-cid 88"),
        )?;

        if !stderr.is_empty() {
            println!("{stderr}");
            if !stderr.contains("Started enclave with enclave-cid") {
                return Err(anyhow!("Error running enclave image: {stderr}"));
            }
        }

        println!("Enclave running");

        Ok(())
    }

    /* AWS EC2 UTILITY */

    pub async fn get_instance_ip(&self, instance_id: &str, region: String) -> Result<String> {
        Ok(self
            .client(region)
            .await
            .describe_instances()
            .filters(
                aws_sdk_ec2::model::Filter::builder()
                    .name("instance-id")
                    .values(instance_id)
                    .build(),
            )
            .send()
            .await
            .context("could not describe instances")?
            // response parsing from here
            .reservations()
            .ok_or(anyhow!("could not parse reservations"))?
            .first()
            .ok_or(anyhow!("no reservation found"))?
            .instances()
            .ok_or(anyhow!("could not parse instances"))?
            .first()
            .ok_or(anyhow!("no instances with the given id"))?
            .public_ip_address()
            .ok_or(anyhow!("could not parse ip address"))?
            .to_string())
    }

    pub async fn launch_instance(
        &self,
        job: String,
        instance_type: aws_sdk_ec2::model::InstanceType,
        image_url: &str,
        architecture: &str,
        region: String,
        contract_address: String,
        chain_id: String,
    ) -> Result<String> {
        let req_client = reqwest::Client::builder()
            .no_gzip()
            .timeout(Duration::from_secs(10))
            .build()
            .context("failed to build reqwest client")?;
        let size = req_client
            .head(image_url)
            .send()
            .await
            .context("failed to fetch eif file header")?
            .headers()["content-length"]
            .to_str()
            .context("could not stringify content length")?
            .parse::<usize>()
            .context("failed to parse content length")?
            / 1000000000;

        println!("eif size: {size} GB");
        // limit enclave image size
        if size > 8 {
            return Err(anyhow!("enclave image too big"));
        }

        let instance_ami = self
            .get_amis(region.clone(), architecture)
            .await
            .context("could not get amis")?;

        let enclave_options = aws_sdk_ec2::model::EnclaveOptionsRequest::builder()
            .set_enabled(Some(true))
            .build();
        let ebs = aws_sdk_ec2::model::EbsBlockDevice::builder()
            .volume_size(12)
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
        let chain_tag = aws_sdk_ec2::model::Tag::builder()
            .set_key(Some("chainID".to_string()))
            .set_value(Some(chain_id))
            .build();
        let contract_tag = aws_sdk_ec2::model::Tag::builder()
            .set_key(Some("contractAddress".to_string()))
            .set_value(Some(contract_address))
            .build();
        let tags = aws_sdk_ec2::model::TagSpecification::builder()
            .set_resource_type(Some(aws_sdk_ec2::model::ResourceType::Instance))
            .tags(name_tag)
            .tags(managed_tag)
            .tags(job_tag)
            .tags(project_tag)
            .tags(chain_tag)
            .tags(contract_tag)
            .build();
        let subnet = self
            .get_subnet(region.clone())
            .await
            .context("could not get subnet")?;
        let sec_group = self
            .get_security_group(region.clone())
            .await
            .context("could not get subnet")?;

        Ok(self
            .client(region)
            .await
            .run_instances()
            .set_image_id(Some(instance_ami))
            .set_instance_type(Some(instance_type))
            .set_key_name(Some(self.key_name.clone()))
            .set_min_count(Some(1))
            .set_max_count(Some(1))
            .set_enclave_options(Some(enclave_options))
            .block_device_mappings(block_device_mapping)
            .tag_specifications(tags)
            .security_group_ids(sec_group)
            .subnet_id(subnet)
            .send()
            .await
            .context("could not run instance")?
            // response parsing from here
            .instances()
            .ok_or(anyhow!("could not parse instances"))?
            .first()
            .ok_or(anyhow!("no instance found"))?
            .instance_id()
            .ok_or(anyhow!("could not parse group id"))?
            .to_string())
    }

    async fn terminate_instance(&self, instance_id: &str, region: String) -> Result<()> {
        let _ = self
            .client(region)
            .await
            .terminate_instances()
            .instance_ids(instance_id)
            .send()
            .await
            .context("could not terminate instance")?;

        Ok(())
    }

    async fn get_amis(&self, region: String, architecture: &str) -> Result<String> {
        let project_filter = aws_sdk_ec2::model::Filter::builder()
            .name("tag:project")
            .values("oyster")
            .build();
        let name_filter = aws_sdk_ec2::model::Filter::builder()
            .name("name")
            .values("marlin/oyster/worker-".to_owned() + architecture + "-????????")
            .build();

        let own_ami = self
            .client(region.clone())
            .await
            .describe_images()
            .owners("self")
            .filters(project_filter)
            .filters(name_filter)
            .send()
            .await
            .context("could not describe images")?;

        let own_ami = own_ami
            .images()
            .ok_or(anyhow!("could not parse images"))?
            .first();

        if own_ami.is_some() {
            Ok(own_ami
                .unwrap()
                .image_id()
                .ok_or(anyhow!("could not parse image id"))?
                .to_string())
        } else {
            self.get_community_amis(region, architecture)
                .await
                .context("could not get community ami")
        }
    }

    async fn get_community_amis(&self, region: String, architecture: &str) -> Result<String> {
        let owner = "753722448458";
        let name_filter = aws_sdk_ec2::model::Filter::builder()
            .name("name")
            .values("marlin/oyster/worker-".to_owned() + architecture + "-????????")
            .build();

        Ok(self
            .client(region)
            .await
            .describe_images()
            .owners(owner)
            .filters(name_filter)
            .send()
            .await
            .context("could not describe images")?
            // response parsing from here
            .images()
            .ok_or(anyhow!("could not parse images"))?
            .first()
            .ok_or(anyhow!("no images found"))?
            .image_id()
            .ok_or(anyhow!("could not parse image id"))?
            .to_string())
    }

    pub async fn get_security_group(&self, region: String) -> Result<String> {
        let filter = aws_sdk_ec2::model::Filter::builder()
            .name("tag:project")
            .values("oyster")
            .build();

        Ok(self
            .client(region)
            .await
            .describe_security_groups()
            .filters(filter)
            .send()
            .await
            .context("could not describe security groups")?
            // response parsing from here
            .security_groups()
            .ok_or(anyhow!("could not parse security groups"))?
            .first()
            .ok_or(anyhow!("no security group found"))?
            .group_id()
            .ok_or(anyhow!("could not parse group id"))?
            .to_string())
    }

    pub async fn get_subnet(&self, region: String) -> Result<String> {
        let filter = aws_sdk_ec2::model::Filter::builder()
            .name("tag:project")
            .values("oyster")
            .build();

        Ok(self
            .client(region)
            .await
            .describe_subnets()
            .filters(filter)
            .send()
            .await
            .context("could not describe subnets")?
            // response parsing from here
            .subnets()
            .ok_or(anyhow!("Could not parse subnets"))?
            .first()
            .ok_or(anyhow!("no subnet found"))?
            .subnet_id()
            .ok_or(anyhow!("Could not parse subnet id"))?
            .to_string())
    }

    pub async fn get_job_instance_id(
        &self,
        job: &str,
        region: String,
    ) -> Result<(bool, String, String)> {
        let res = self
            .client(region)
            .await
            .describe_instances()
            .filters(
                aws_sdk_ec2::model::Filter::builder()
                    .name("tag:jobId")
                    .values(job)
                    .build(),
            )
            .send()
            .await
            .context("could not describe instances")?;
        // response parsing from here
        let reservations = res
            .reservations()
            .ok_or(anyhow!("could not parse reservations"))?;

        if reservations.is_empty() {
            Ok((false, "".to_owned(), "".to_owned()))
        } else {
            let instance = reservations[0]
                .instances()
                .ok_or(anyhow!("could not parse instances"))?
                .first()
                .ok_or(anyhow!("instance not found"))?;
            Ok((
                true,
                instance
                    .instance_id()
                    .ok_or(anyhow!("could not parse ip address"))?
                    .to_string(),
                instance
                    .state()
                    .ok_or(anyhow!("could not parse instance state"))?
                    .name()
                    .ok_or(anyhow!("could not parse instance state name"))?
                    .as_str()
                    .to_owned(),
            ))
        }
    }

    pub async fn get_instance_state(&self, instance_id: &str, region: String) -> Result<String> {
        Ok(self
            .client(region)
            .await
            .describe_instances()
            .filters(
                aws_sdk_ec2::model::Filter::builder()
                    .name("instance-id")
                    .values(instance_id)
                    .build(),
            )
            .send()
            .await
            .context("could not describe instances")?
            // response parsing from here
            .reservations()
            .ok_or(anyhow!("could not parse reservations"))?
            .first()
            .ok_or(anyhow!("no reservation found"))?
            .instances()
            .ok_or(anyhow!("could not parse instances"))?
            .first()
            .ok_or(anyhow!("no instances with the given id"))?
            .state()
            .ok_or(anyhow!("could not parse instance state"))?
            .name()
            .ok_or(anyhow!("could not parse instance state name"))?
            .as_str()
            .into())
    }

    pub async fn get_enclave_state(&self, instance_id: &str, region: String) -> Result<String> {
        let public_ip_address = self
            .get_instance_ip(instance_id, region.clone())
            .await
            .context("could not fetch instance ip")?;
        let sess = self
            .ssh_connect(&(public_ip_address + ":22"))
            .await
            .context("error establishing ssh connection")?;

        let (stdout, stderr) = Self::ssh_exec(&sess, "nitro-cli describe-enclaves")
            .context("could not describe enclaves")?;
        if !stderr.is_empty() {
            println!("{stderr}");
            return Err(anyhow!("Error describing enclaves: {stderr}"));
        }

        let enclave_data: Vec<HashMap<String, Value>> =
            serde_json::from_str(&stdout).context("could not parse enclave description")?;

        Ok(enclave_data
            .get(0)
            .and_then(|data| data.get("State").and_then(Value::as_str))
            .unwrap_or("No state found")
            .to_owned())
    }

    pub async fn get_enclave_image_prc(&self, instance_id: &str, region: String) -> Result<PRC> {
        let public_ip_address = self
            .get_instance_ip(instance_id, region.clone())
            .await
            .context("could not fetch instance ip")?;
        let sess = self
            .ssh_connect(&(public_ip_address + ":22"))
            .await
            .context("error establishing ssh connection")?;

        let (stdout, stderr) =
            Self::ssh_exec(&sess, "nitro-cli describe-eif --eif-path enclave.eif")
                .context("could not describe eif")?;
        if !stderr.is_empty() {
            println!("{stderr}");
            return Err(anyhow!("Error describing eif image: {stderr}"));
        }

        let eif_data: HashMap<String, HashMap<String, String>> = serde_json::from_str(&stdout)
            .context("could not parse enclave image eif description.")?;

        if let Some(measurement) = eif_data.get("Measurements") {
            let prc = PRC {
                prc0: measurement.get("prc0").unwrap().to_owned(),
                prc1: measurement.get("prc1").unwrap().to_owned(),
                prc2: measurement.get("prc2").unwrap().to_owned(),
            };
            return Ok(prc);
        }
        Err(anyhow!("Could not find measurements in eif image"))
    }

    async fn allocate_ip_addr(&self, job: String, region: String) -> Result<(String, String)> {
        let (exist, alloc_id, public_ip) = self
            .get_job_elastic_ip(&job, region.clone())
            .await
            .context("could not get elastic ip for job")?;

        if exist {
            println!("Elastic Ip already exists");
            return Ok((alloc_id, public_ip));
        }

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
            .set_resource_type(Some(aws_sdk_ec2::model::ResourceType::ElasticIp))
            .tags(managed_tag)
            .tags(job_tag)
            .tags(project_tag)
            .build();

        let resp = self
            .client(region)
            .await
            .allocate_address()
            .domain(aws_sdk_ec2::model::DomainType::Vpc)
            .tag_specifications(tags)
            .send()
            .await
            .context("could not allocate elastic ip")?;

        Ok((
            resp.allocation_id()
                .ok_or(anyhow!("could not parse allocation id"))?
                .to_string(),
            resp.public_ip()
                .ok_or(anyhow!("could not parse public ip"))?
                .to_string(),
        ))
    }

    async fn get_job_elastic_ip(
        &self,
        job: &str,
        region: String,
    ) -> Result<(bool, String, String)> {
        let filter_a = aws_sdk_ec2::model::Filter::builder()
            .name("tag:project")
            .values("oyster")
            .build();

        let filter_b = aws_sdk_ec2::model::Filter::builder()
            .name("tag:jobId")
            .values(job)
            .build();

        Ok(
            match self
                .client(region)
                .await
                .describe_addresses()
                .filters(filter_a)
                .filters(filter_b)
                .send()
                .await
                .context("could not describe elastic ips")?
                // response parsing starts here
                .addresses()
                .ok_or(anyhow!("could not parse addresses"))?
                .first()
            {
                None => (false, String::new(), String::new()),
                Some(addrs) => (
                    true,
                    addrs
                        .allocation_id()
                        .ok_or(anyhow!("could not parse allocation id"))?
                        .to_string(),
                    addrs
                        .public_ip()
                        .ok_or(anyhow!("could not parse public ip"))?
                        .to_string(),
                ),
            },
        )
    }

    async fn get_instance_elastic_ip(
        &self,
        instance: &str,
        region: String,
    ) -> Result<(bool, String, String)> {
        let filter_a = aws_sdk_ec2::model::Filter::builder()
            .name("instance-id")
            .values(instance)
            .build();

        Ok(
            match self
                .client(region)
                .await
                .describe_addresses()
                .filters(filter_a)
                .send()
                .await
                .context("could not describe elastic ips")?
                // response parsing starts here
                .addresses()
                .ok_or(anyhow!("could not parse addresses"))?
                .first()
            {
                None => (false, String::new(), String::new()),
                Some(addrs) => (
                    true,
                    addrs
                        .allocation_id()
                        .ok_or(anyhow!("could not parse allocation id"))?
                        .to_string(),
                    addrs
                        .association_id()
                        .ok_or(anyhow!("could not parse public ip"))?
                        .to_string(),
                ),
            },
        )
    }

    async fn associate_address(
        &self,
        instance_id: &str,
        alloc_id: &str,
        region: String,
    ) -> Result<()> {
        self.client(region)
            .await
            .associate_address()
            .allocation_id(alloc_id)
            .instance_id(instance_id)
            .send()
            .await
            .context("could not associate elastic ip")?;
        Ok(())
    }

    async fn disassociate_address(&self, association_id: &str, region: String) -> Result<()> {
        self.client(region)
            .await
            .disassociate_address()
            .association_id(association_id)
            .send()
            .await
            .context("could not disassociate elastic ip")?;
        Ok(())
    }

    async fn release_address(&self, alloc_id: &str, region: String) -> Result<()> {
        self.client(region)
            .await
            .release_address()
            .allocation_id(alloc_id)
            .send()
            .await
            .context("could not release elastic ip")?;
        Ok(())
    }

    pub async fn spin_up_instance(
        &self,
        image_url: &str,
        job: String,
        instance_type: &str,
        region: String,
        req_mem: i64,
        req_vcpu: i32,
        bandwidth: u64,
        contract_address: String,
        chain_id: String,
    ) -> Result<String> {
        let instance_type = aws_sdk_ec2::model::InstanceType::from_str(instance_type)
            .context("cannot parse instance type")?;
        let resp = self
            .client(region.clone())
            .await
            .describe_instance_types()
            .instance_types(instance_type.clone())
            .send()
            .await
            .context("could not describe instance types")?;
        let mut architecture = "amd64".to_string();
        let mut v_cpus: i32 = 4;
        let mut mem: i64 = 8192;

        let instance_types = resp
            .instance_types()
            .ok_or(anyhow!("error fetching instance info"))?;
        for instance in instance_types {
            let supported_architectures = instance
                .processor_info()
                .ok_or(anyhow!("error fetching instance processor info"))?
                .supported_architectures()
                .ok_or(anyhow!("error fetching instance architecture info"))?;
            if let Some(arch) = supported_architectures.iter().next() {
                if arch.as_str() == "x86_64" {
                    architecture = "amd64".to_owned();
                } else {
                    architecture = "arm64".to_owned();
                }
                println!("architecture: {}", arch.as_str());
            }
            v_cpus = instance
                .v_cpu_info()
                .ok_or(anyhow!("error fetching instance v_cpu info"))?
                .default_v_cpus()
                .ok_or(anyhow!("error fetching instance v_cpu info"))?;
            println!("v_cpus: {v_cpus}");
            mem = instance
                .memory_info()
                .ok_or(anyhow!("error fetching instance memory info"))?
                .size_in_mi_b()
                .ok_or(anyhow!("error fetching instance v_cpu info"))?;
            println!("memory: {mem}");
        }

        if req_mem > mem || req_vcpu > v_cpus {
            return Err(anyhow!("Required memory or vcpus are more than available"));
        }
        let instance = self
            .launch_instance(
                job.clone(),
                instance_type,
                image_url,
                &architecture,
                region.clone(),
                contract_address,
                chain_id,
            )
            .await
            .context("could not launch instance")?;
        sleep(Duration::from_secs(100)).await;

        let res = self
            .post_spin_up(
                image_url,
                job.clone(),
                &instance,
                region.clone(),
                req_mem,
                req_vcpu,
                bandwidth,
            )
            .await;

        if let Err(err) = res {
            println!("error during post spin up: {err:?}");
            self.spin_down_instance(&instance, &job, region.clone())
                .await
                .context("could not spin down instance after error during post spin up")?;
            return Err(err).context("error during post spin up");
        }
        Ok(instance)
    }

    async fn post_spin_up(
        &self,
        image_url: &str,
        job: String,
        instance: &str,
        region: String,
        req_mem: i64,
        req_vcpu: i32,
        bandwidth: u64,
    ) -> Result<()> {
        let (alloc_id, ip) = self
            .allocate_ip_addr(job.clone(), region.clone())
            .await
            .context("error allocating ip address")?;
        println!("Elastic Ip allocated: {ip}");

        self.associate_address(instance, &alloc_id, region.clone())
            .await
            .context("could not associate ip address")?;
        self.run_enclave_impl(instance, region, image_url, req_vcpu, req_mem, bandwidth)
            .await
            .context("could not run enclave")?;
        Ok(())
    }

    pub async fn spin_down_instance(
        &self,
        instance_id: &str,
        job: &str,
        region: String,
    ) -> Result<()> {
        let (exist, _, association_id) = self
            .get_instance_elastic_ip(instance_id, region.clone())
            .await
            .context("could not get elastic ip of instance")?;
        if exist {
            self.disassociate_address(association_id.as_str(), region.clone())
                .await
                .context("could not disassociate address")?;
        }
        let (exist, alloc_id, _) = self
            .get_job_elastic_ip(job, region.clone())
            .await
            .context("could not get elastic ip of job")?;
        if exist {
            self.release_address(alloc_id.as_str(), region.clone())
                .await
                .context("could not release address")?;
            println!("Elastic IP released");
        }

        self.terminate_instance(instance_id, region)
            .await
            .context("could not terminate instance")?;
        Ok(())
    }
}

#[async_trait]
impl InfraProvider for Aws {
    async fn spin_up(
        &mut self,
        eif_url: &str,
        job: String,
        instance_type: &str,
        region: String,
        req_mem: i64,
        req_vcpu: i32,
        bandwidth: u64,
        contract_address: String,
        chain_id: String,
    ) -> Result<String> {
        let instance = self
            .spin_up_instance(
                eif_url,
                job,
                instance_type,
                region,
                req_mem,
                req_vcpu,
                bandwidth,
                contract_address,
                chain_id,
            )
            .await
            .context("could not spin up instance")?;
        Ok(instance)
    }

    async fn spin_down(&mut self, instance_id: &str, job: String, region: String) -> Result<bool> {
        let _ = self
            .spin_down_instance(instance_id, &job, region)
            .await
            .context("could not spin down instance")?;
        Ok(true)
    }

    async fn get_job_instance(&self, job: &str, region: String) -> Result<(bool, String, String)> {
        Ok(self
            .get_job_instance_id(job, region)
            .await
            .context("could not get instance id for job")?)
    }

    async fn get_job_ip(&self, job_id: &str, region: String) -> Result<String> {
        let instance = self
            .get_job_instance(job_id, region.clone())
            .await
            .context("could not get instance id for job instance ip")?;

        if !instance.0 {
            return Err(anyhow!("Instance not found for job - {job_id}"));
        }

        Ok(self
            .get_instance_ip(&instance.1, region)
            .await
            .context("could not get instance ip")?)
    }

    async fn check_instance_running(&mut self, instance_id: &str, region: String) -> Result<bool> {
        let res = self
            .get_instance_state(instance_id, region)
            .await
            .context("could not get current instance state")?;
        Ok(res == "running" || res == "pending")
    }

    async fn get_job_enclave_state(&self, job_id: &str, region: String) -> Result<(String, PRC)> {
        let instance = self
            .get_job_instance(job_id, region.clone())
            .await
            .context("could not get instance id for job enclave state")?;

        if !instance.0 {
            return Err(anyhow!("Instance not found for job - {job_id}"));
        }

        let state = self
            .get_enclave_state(&instance.1, region.clone())
            .await
            .context("could not get current enclace state")?;

        let prc = self.get_enclave_image_prc(&instance.1, region).await?;

        Ok((state, prc))
    }

    async fn check_enclave_running(&mut self, instance_id: &str, region: String) -> Result<bool> {
        let res = self
            .get_enclave_state(instance_id, region)
            .await
            .context("could not get current enclace state")?;
        // There can be 2 states - RUNNING or TERMINATING
        Ok(res == "RUNNING")
    }

    async fn run_enclave(
        &mut self,
        _job: String,
        instance_id: &str,
        region: String,
        image_url: &str,
        req_vcpu: i32,
        req_mem: i64,
        bandwidth: u64,
    ) -> Result<()> {
        self.run_enclave_impl(instance_id, region, image_url, req_vcpu, req_mem, bandwidth)
            .await
            .context("could not run enclave")?;
        Ok(())
    }
}
