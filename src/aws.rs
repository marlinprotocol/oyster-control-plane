use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use aws_types::region::Region;
use serde_json::Value;
use ssh2::Session;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::{BufReader, Read};
use std::net::TcpStream;
use std::path::Path;
use std::process::Command;
use std::str::FromStr;
use tokio::time::{sleep, Duration};
use whoami::username;

use crate::market::InfraProvider;

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

    /* AWS KEY PAIR UTILITY */
    pub async fn generate_key_pair(&self) -> Result<()> {
        let priv_check = Path::new(&self.key_location).exists();
        let pub_check = Path::new(&self.pub_key_location).exists();
        if priv_check && pub_check {
            Ok(())
        } else if priv_check {
            let output = Command::new("ssh-keygen")
                .arg("-y")
                .arg("-f")
                .arg(&self.key_location)
                .arg(">")
                .arg(&self.pub_key_location)
                .output()?;

            if output.status.success() {
                Ok(())
            } else {
                Err(anyhow!("Failed to generate key pair"))
            }
        } else {
            let output = Command::new("ssh-keygen")
                .arg("-t")
                .arg("ed25519")
                .arg("-f")
                .arg(&self.key_location)
                .arg("-N")
                .arg("")
                .output()?;

            if output.status.success() {
                Ok(())
            } else {
                Err(anyhow!("Failed to generate key pair"))
            }
        }
    }

    pub async fn key_setup(&self, region: String) -> Result<()> {
        let key_check = self
            .check_key_pair(region.clone())
            .await
            .context("failed to check key pair")?;

        if !key_check {
            self.import_key_pair(region).await?;
        } else {
            println!("found existing keypair and pem file, skipping key setup");
        }

        Ok(())
    }

    pub async fn import_key_pair(&self, region: String) -> Result<()> {
        let f = File::open(&self.pub_key_location)?;
        let mut reader = BufReader::new(f);
        let mut buffer = Vec::new();

        reader.read_to_end(&mut buffer)?;

        self.client(region)
            .await
            .import_key_pair()
            .key_name(&self.key_name)
            .public_key_material(aws_sdk_ec2::types::Blob::new(buffer))
            .send()
            .await?;

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

    async fn run_enclave_impl(
        &self,
        sess: &Session,
        url: &str,
        v_cpus: i32,
        mem: i64,
        bandwidth: u64,
    ) -> Result<()> {
        let mut channel = sess.channel_session()?;
        let mut s = String::new();
        channel.exec(
            &("echo -e '---\\nmemory_mib: ".to_owned()
                + &((mem).to_string())
                + "\\ncpu_count: "
                + &((v_cpus).to_string())
                + "' > /home/ubuntu/allocator_new.yaml"),
        )?;
        let _ = channel.stderr().read_to_string(&mut s);
        let _ = channel.wait_close();

        channel = sess.channel_session()?;
        channel.exec("sudo apt-get update -y")?;
        let _ = channel.stderr().read_to_string(&mut s);
        let _ = channel.wait_close();

        channel = sess.channel_session()?;
        channel
            .exec("sudo cp /home/ubuntu/allocator_new.yaml /etc/nitro_enclaves/allocator.yaml")?;

        let _ = channel.stderr().read_to_string(&mut s);
        let _ = channel.wait_close();

        s.clear();

        channel = sess.channel_session()?;
        channel.exec("sudo systemctl restart nitro-enclaves-allocator.service")?;
        let _ = channel.stderr().read_to_string(&mut s);
        let _ = channel.wait_close();
        if !s.is_empty() {
            println!("{s}");
            return Err(anyhow!("Error starting nitro-anclaves-allocator service"));
        }
        s.clear();

        println!("Nitro Enclave Service set up with cpus: {v_cpus} and memory: {mem}");

        channel = sess.channel_session()?;
        channel.exec(&("wget -O enclave.eif ".to_owned() + url))?;
        let _ = channel.stderr().read_to_string(&mut s);
        let _ = channel.wait_close();
        s.clear();

        if self.whitelist.as_str() != "" || self.blacklist.as_str() != "" {
            channel = sess.channel_session()?;
            channel.exec("sha256sum /home/ubuntu/enclave.eif")?;
            let _ = channel.stderr().read_to_string(&mut s);
            let _ = channel.wait_close();
            if !s.is_empty() {
                println!("{s}");
                return Err(anyhow!("Error calculating hash of enclave image"));
            }
            s.clear();

            if let Some(line) = s.split_whitespace().next() {
                println!("Hash: {line}");
                if self.whitelist.as_str() != "" {
                    println!("Checking whitelist...");
                    let file_path = self.whitelist.as_str();
                    let contents = fs::read_to_string(file_path);

                    if let Err(err) = contents {
                        println!("Error reading whitelist file: {err:?}");
                        return Err(anyhow!("Error reading whitelist file"));
                    } else {
                        let contents = contents.unwrap();
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
                }
                if self.blacklist.as_str() != "" {
                    println!("Checking blacklist...");
                    let file_path = self.blacklist.as_str();
                    let contents = fs::read_to_string(file_path);

                    if let Err(err) = contents {
                        println!("Error reading blacklist file: {err:?}");
                        return Err(anyhow!("Error reading blacklist file"));
                    } else {
                        let contents = contents.unwrap();
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
            }
        }

        let mut output = String::new();
        channel = sess.channel_session()?;
        channel.exec("nmcli device status")?;
        let _ = channel.stderr().read_to_string(&mut s);
        let _ = channel.read_to_string(&mut output);
        let _ = channel.wait_close();
        if !s.is_empty() || output.is_empty() {
            println!("{s}");
            return Err(anyhow!("Error fetching network interface name"));
        }
        s.clear();
        let mut interface = String::new();
        let entries: Vec<&str> = output.split('\n').collect();
        for line in entries {
            let entry: Vec<&str> = line.split_whitespace().collect();
            if entry.len() > 1 && entry[1] == "ethernet" {
                interface = entry[0].to_string();
                break;
            }
        }
        output.clear();

        if !interface.is_empty() {
            channel = sess.channel_session()?;
            channel.exec(&("sudo tc qdisc show dev ".to_owned() + &interface + " root"))?;
            let _ = channel.stderr().read_to_string(&mut s);
            let _ = channel.read_to_string(&mut output);
            let _ = channel.wait_close();
            if !s.is_empty() || output.is_empty() {
                println!("{s}");
                return Err(anyhow!(
                    "Error fetching network interface qdisc configuration."
                ));
            }
            s.clear();
            let entries: Vec<&str> = output.trim().split('\n').collect();
            let mut is_qdisc_config_set = false;
            for entry in entries {
                if entry.contains("tbf")
                    && entry
                        .to_lowercase()
                        .contains(&format!("rate {}mbit burst 4000mb lat 100ms", bandwidth))
                {
                    println!("Bandwidth limit already set");
                    is_qdisc_config_set = true;
                    break;
                }
            }
            output.clear();

            if !is_qdisc_config_set {
                channel = sess.channel_session()?;
                channel.exec(
                    &("sudo tc qdisc add dev ".to_owned()
                        + &interface
                        + " root tbf rate "
                        + &bandwidth.to_string()
                        + "mbit burst 4000Mb latency 100ms"),
                )?;

                let _ = channel.stderr().read_to_string(&mut s);
                let _ = channel.wait_close();

                if !s.is_empty() {
                    println!("{s}");
                    return Err(anyhow!("Error setting up bandwidth limit"));
                }
                s.clear();
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
        let mut channel = sess.channel_session()?;
        channel.exec("sudo iptables -t nat -S PREROUTING")?;

        let _ = channel.stderr().read_to_string(&mut s);
        let _ = channel.read_to_string(&mut output);
        let _ = channel.wait_close();

        if !s.is_empty() || output.is_empty() {
            println!("{}", s);
            return Err(anyhow!("Failed to get iptables rules"));
        }
        s.clear();

        let rules: Vec<&str> = output.trim().split('\n').map(|s| s.trim()).collect();

        if rules[0] != iptables_rules[0] {
            println!("Got '{}' instead of '{}'", rules[0], iptables_rules[0]);
            return Err(anyhow!("Failed to get PREROUTING ACCEPT rules"));
        }

        if !rules.contains(&iptables_rules[1]) {
            channel = sess.channel_session()?;
            channel
                .exec(
                    "sudo iptables -A PREROUTING -t nat -p tcp --dport 80 -i ens5 -j REDIRECT --to-port 1200",
                )?;
            let _ = channel.stderr().read_to_string(&mut s);
            let _ = channel.wait_close();
        }

        if !rules.contains(&iptables_rules[2]) {
            channel = sess.channel_session()?;
            channel
            .exec(
                "sudo iptables -A PREROUTING -t nat -p tcp --dport 443 -i ens5 -j REDIRECT --to-port 1200",
            )?;
            let _ = channel.stderr().read_to_string(&mut s);
            let _ = channel.wait_close();
        }

        if !rules.contains(&iptables_rules[3]) {
            channel = sess.channel_session()?;
            channel
            .exec(
                "sudo iptables -A PREROUTING -t nat -p tcp --dport 1025:65535 -i ens5 -j REDIRECT --to-port 1200",
            )?;
            let _ = channel.stderr().read_to_string(&mut s);
            let _ = channel.wait_close();
        }

        output.clear();

        if !s.is_empty() {
            println!("{s}");
            return Err(anyhow!("Error setting up proxies"));
        }
        s.clear();

        channel = sess.channel_session()?;
        channel.exec(
            &("nitro-cli run-enclave --cpu-count ".to_owned()
                + &((v_cpus).to_string())
                + " --memory "
                + &((mem).to_string())
                + " --eif-path enclave.eif --enclave-cid 88"),
        )?;

        let _ = channel.stderr().read_to_string(&mut s);
        let _ = channel.wait_close();
        if !s.is_empty() {
            println!("{s}");
            if !s.contains("Started enclave with enclave-cid") {
                return Err(anyhow!("Error running enclave image"));
            }
        }

        println!("Enclave running");
        Ok(())
    }

    pub async fn run_enclave(
        &self,
        job: String,
        instance_id: &str,
        region: String,
        image_url: &str,
        req_vcpu: i32,
        req_mem: i64,
        bandwidth: u64,
    ) -> Result<()> {
        let res = self.get_instance_ip(instance_id, region.clone()).await;
        if let Err(err) = res {
            self.spin_down_instance(instance_id, &job, region.clone())
                .await?;
            return Err(anyhow!("error launching instance, {err:?}"));
        }
        let mut public_ip_address = res.unwrap();
        if public_ip_address.is_empty() {
            self.spin_down_instance(instance_id, &job, region.clone())
                .await?;
            return Err(anyhow!("error fetching instance ip address"));
        }
        public_ip_address.push_str(":22");
        let sess = self.ssh_connect(&public_ip_address).await;
        match sess {
            Ok(r) => {
                let res = self
                    .run_enclave_impl(&r, image_url, req_vcpu, req_mem, bandwidth)
                    .await;
                match res {
                    Ok(_) => {}
                    Err(e) => {
                        self.spin_down_instance(instance_id, &job, region.clone())
                            .await?;
                        println!("Error running enclave: {e:?}");
                        return Err(anyhow!(
                            "error running enclave, terminating launched instance"
                        ));
                    }
                }
            }
            Err(_) => {
                self.spin_down_instance(instance_id, &job, region.clone())
                    .await?;
                return Err(anyhow!("error establishing ssh connection"));
            }
        }
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
            .await?
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
    ) -> Result<String> {
        let size: i64;
        let req_client = reqwest::Client::builder().no_gzip().build();
        match req_client {
            Ok(req_client) => {
                let res = req_client.head(image_url).send().await;
                match res {
                    Ok(res) => {
                        let content_len = res.headers()["content-length"].to_str()?;
                        size = content_len.parse::<i64>()? / 1000000;
                    }
                    Err(e) => return Err(anyhow!("failed to fetch eif file header, {e}")),
                }
            }
            Err(e) => return Err(anyhow!("failed to fetch eif file header, {e}")),
        }

        println!("eif size: {size} MB");
        let size = size / 1000;
        let mut sdd = 15;
        if size > sdd {
            sdd = size + 10;
        }

        let instance_ami = self.get_amis(region.clone(), architecture).await?;

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
        let subnet = self.get_subnet(region.clone()).await?;
        let sec_group = self.get_security_group(region.clone()).await?;

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
            .await?
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
            .await?;

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
            .await?;

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
            self.get_community_amis(region, architecture).await
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
            .await?
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
            .await?
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
            .await?
            // response parsing from here
            .subnets()
            .ok_or(anyhow!("Could not parse subnets"))?
            .first()
            .ok_or(anyhow!("no subnet found"))?
            .subnet_id()
            .ok_or(anyhow!("Could not parse subnet id"))?
            .to_string())
    }

    pub async fn get_job_instance_id(&self, job: &str, region: String) -> Result<(String, String)> {
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
            .await?;
        // response parsing from here
        let instance = res
            .reservations()
            .ok_or(anyhow!("could not parse reservations"))?
            .first()
            .ok_or(anyhow!("reservation not found"))?
            .instances()
            .ok_or(anyhow!("could not parse instances"))?
            .first()
            .ok_or(anyhow!("no instances for the given job"))?;

        Ok((
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
            .await?
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
        let res = self.get_instance_ip(&instance_id, region.clone()).await;
        let mut public_ip_address = res.unwrap();
        if public_ip_address.is_empty() {
            return Err(anyhow!("error fetching instance ip address"));
        }
        public_ip_address.push_str(":22");

        let sess = self.ssh_connect(&public_ip_address).await;
        match sess {
            Ok(sess) => {
                let mut channel = sess.channel_session()?;
                channel.exec("nitro-cli describe-enclaves")?;
                let mut command_output = String::new();
                channel.read_to_string(&mut command_output)?;

                let enclave_data: Vec<HashMap<String, Value>> =
                    serde_json::from_str(&command_output)?;
                let _ = channel.close();

                let enclave_status = match enclave_data
                    .get(0)
                    .and_then(|data| data.get("State").and_then(Value::as_str))
                {
                    Some(status) => status.to_owned(),
                    None => "No state found".to_owned(),
                };
                Ok(enclave_status)
            }
            Err(_) => Err(anyhow!("error establishing ssh connection")),
        }
    }

    async fn allocate_ip_addr(&self, job: String, region: String) -> Result<(String, String)> {
        let (exist, alloc_id, public_ip) = self.get_job_elastic_ip(&job, region.clone()).await?;

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
            .await?;

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
                .await?
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
                .await?
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
            .await?;
        Ok(())
    }

    async fn disassociate_address(&self, association_id: &str, region: String) -> Result<()> {
        self.client(region)
            .await
            .disassociate_address()
            .association_id(association_id)
            .send()
            .await?;
        Ok(())
    }

    async fn release_address(&self, alloc_id: &str, region: String) -> Result<()> {
        self.client(region)
            .await
            .release_address()
            .allocation_id(alloc_id)
            .send()
            .await?;
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
    ) -> Result<String> {
        let ec2_type = aws_sdk_ec2::model::InstanceType::from_str(instance_type)?;
        let resp = self
            .client(region.clone())
            .await
            .describe_instance_types()
            .instance_types(ec2_type)
            .send()
            .await?;
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
        let instance_type = aws_sdk_ec2::model::InstanceType::from_str(instance_type)?;
        let instance = self
            .launch_instance(
                job.clone(),
                instance_type,
                image_url,
                &architecture,
                region.clone(),
            )
            .await?;
        sleep(Duration::from_secs(100)).await;
        let res = self.allocate_ip_addr(job.clone(), region.clone()).await;
        if let Err(err) = res {
            self.spin_down_instance(&instance, &job, region.clone())
                .await?;
            return Err(anyhow!("error launching instance, {err:?}"));
        }
        let (alloc_id, ip) = res.unwrap();
        println!("Elastic Ip allocated: {ip}");

        let res = self
            .associate_address(&instance, &alloc_id, region.clone())
            .await;
        if let Err(err) = res {
            self.spin_down_instance(&instance, &job, region.clone())
                .await?;
            return Err(anyhow!("error launching instance, {err:?}"));
        }
        let res = self
            .run_enclave(
                job, &instance, region, image_url, req_vcpu, req_mem, bandwidth,
            )
            .await;
        match res {
            Ok(_) => {
                return Ok(instance);
            }
            Err(err) => {
                println!("Enclave failed to start, {err:?}");
                return Err(anyhow!("error running enclave on instance, {err:?}"));
            }
        }
    }

    pub async fn spin_down_instance(
        &self,
        instance_id: &str,
        job: &str,
        region: String,
    ) -> Result<()> {
        let (exist, _, association_id) = self
            .get_instance_elastic_ip(instance_id, region.clone())
            .await?;
        if exist {
            self.disassociate_address(association_id.as_str(), region.clone())
                .await?;
        }
        let (exist, alloc_id, _) = self.get_job_elastic_ip(job, region.clone()).await?;
        if exist {
            self.release_address(alloc_id.as_str(), region.clone())
                .await?;
            println!("Elastic IP released");
        }

        self.terminate_instance(instance_id, region).await?;
        Ok(())
    }
}

use std::error::Error;

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
    ) -> Result<String, Box<dyn Error + Send + Sync>> {
        let instance = self
            .spin_up_instance(
                eif_url,
                job,
                instance_type,
                region,
                req_mem,
                req_vcpu,
                bandwidth,
            )
            .await?;
        Ok(instance)
    }

    async fn spin_down(
        &mut self,
        instance_id: &str,
        job: String,
        region: String,
    ) -> Result<bool, Box<dyn Error + Send + Sync>> {
        let _ = self.spin_down_instance(instance_id, &job, region).await?;
        Ok(true)
    }

    async fn get_job_instance(
        &mut self,
        job: &str,
        region: String,
    ) -> Result<(bool, String, String), Box<dyn Error + Send + Sync>> {
        let instance = self.get_job_instance_id(job, region).await?;
        Ok((true, instance.0, instance.1))
    }

    async fn check_instance_running(
        &mut self,
        instance_id: &str,
        region: String,
    ) -> Result<bool, Box<dyn Error + Send + Sync>> {
        let res = self.get_instance_state(instance_id, region).await?;
        Ok(res == "running" || res == "pending")
    }

    async fn check_enclave_running(
        &mut self,
        instance_id: &str,
        region: String,
    ) -> Result<bool, Box<dyn Error + Send + Sync>> {
        let res = self.get_enclave_state(instance_id, region).await?;
        // There can be 2 states - RUNNING or TERMINATING
        Ok(res == "RUNNING")
    }

    async fn run_enclave(
        &mut self,
        job: String,
        instance_id: &str,
        region: String,
        image_url: &str,
        req_vcpu: i32,
        req_mem: i64,
        bandwidth: u64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let _ = self
            .run_enclave(
                job,
                instance_id,
                region,
                image_url,
                req_vcpu,
                req_mem,
                bandwidth,
            )
            .await;
        Ok(())
    }
}
