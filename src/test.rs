use anyhow::Result;
use async_trait::async_trait;
use ethers::prelude::rand::Rng;
use ethers::prelude::*;
use ethers::types::Log;
use ethers::utils::keccak256;
use std::collections::HashMap;
use std::fs;
use std::iter;
use std::str::FromStr;
use tokio::time::{Duration, Instant};
use tokio_stream::{Stream, StreamExt};

use crate::market::{GBRateCard, InfraProvider, LogsProvider, RegionalRates};

#[cfg(test)]
#[derive(Clone, Debug)]
pub struct SpinUpOutcome {
    pub time: Instant,
    pub job: String,
    pub instance_type: String,
    pub region: String,
    pub req_mem: i64,
    pub req_vcpu: i32,
    pub bandwidth: u64,
    pub eif_url: String,
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub struct SpinDownOutcome {
    pub time: Instant,
    pub job: String,
    pub _instance_id: String,
    pub region: String,
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub enum TestAwsOutcome {
    SpinUp(SpinUpOutcome),
    SpinDown(SpinDownOutcome),
}

#[cfg(test)]
pub async fn generate_random_string(charset: &[u8], len: usize) -> String {
    let one_char = || charset[rand::thread_rng().gen_range(0..charset.len())] as char;
    iter::repeat_with(one_char).take(len).collect()
}

#[cfg(test)]
pub async fn generate_random_ip() -> String {
    let ip_parts: Vec<u8> = (0..4)
        .map(|_| rand::thread_rng().gen_range(0..=255))
        .collect();
    ip_parts
        .iter()
        .map(|&part| part.to_string())
        .collect::<Vec<String>>()
        .join(".")
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub struct InstanceMetadata {
    pub instance_id: String,
    pub ip_address: String,
}

#[cfg(test)]
impl InstanceMetadata {
    pub async fn new(instance_id: Option<String>, ip_address: Option<String>) -> Self {
        let instance_id =
            instance_id.unwrap_or(generate_random_string(b"1234567890abcdef", 17).await);

        let ip_address = ip_address.unwrap_or(generate_random_ip().await);
        Self {
            instance_id,
            ip_address,
        }
    }
}

#[cfg(test)]
impl Default for InstanceMetadata {
    fn default() -> Self {
        Self {
            instance_id: String::from("i-1234567890abcdef0"),
            ip_address: String::from("123.45.67.89"),
        }
    }
}

#[cfg(test)]
#[derive(Clone, Default)]
pub struct TestAws {
    pub outcomes: Vec<TestAwsOutcome>,
    pub instances: HashMap<String, InstanceMetadata>,
}

#[cfg(test)]
#[async_trait]
impl InfraProvider for TestAws {
    async fn spin_up(
        &mut self,
        eif_url: &str,
        job: String,
        instance_type: &str,
        region: String,
        req_mem: i64,
        req_vcpu: i32,
        bandwidth: u64,
    ) -> Result<String> {
        self.outcomes.push(TestAwsOutcome::SpinUp(SpinUpOutcome {
            time: Instant::now(),
            job: job.clone(),
            instance_type: instance_type.to_owned(),
            region,
            req_mem,
            req_vcpu,
            bandwidth,
            eif_url: eif_url.to_owned(),
        }));

        let res = self.instances.get_key_value(&job);
        if let Some(x) = res {
            return Ok(x.1.instance_id.clone());
        }

        let instance_metadata: InstanceMetadata = InstanceMetadata::new(None, None).await;
        self.instances.insert(job, instance_metadata.clone());

        Ok(instance_metadata.instance_id)
    }

    async fn spin_down(&mut self, instance_id: &str, job: String, region: String) -> Result<bool> {
        self.outcomes
            .push(TestAwsOutcome::SpinDown(SpinDownOutcome {
                time: Instant::now(),
                job: job.clone(),
                _instance_id: instance_id.to_owned(),
                region,
            }));

        self.instances.remove(&job);

        Ok(true)
    }

    async fn get_job_instance(
        &mut self,
        job: &str,
        _region: String,
    ) -> Result<(bool, String, String)> {
        let res = self.instances.get_key_value(job);
        if let Some(x) = res {
            return Ok((true, x.1.instance_id.clone(), "running".to_owned()));
        }

        Ok((false, String::new(), String::new()))
    }

    async fn get_ip_from_instance_id(
        &mut self,
        instance_id: &str,
        _region: String,
    ) -> Result<String> {
        for instance in self.instances.values() {
            if instance.instance_id == instance_id {
                return Ok(instance.ip_address.clone());
            }
        }
        panic!("Instance not found");
    }

    async fn check_instance_running(
        &mut self,
        _instance_id: &str,
        _region: String,
    ) -> Result<bool> {
        // println!("TEST: check_instance_running | instance_id: {}, region: {}", instance_id, region);
        Ok(true)
    }

    async fn check_enclave_running(&mut self, _instance_id: &str, _region: String) -> Result<bool> {
        Ok(true)
    }

    async fn run_enclave(
        &mut self,
        _job: String,
        _instance_id: &str,
        _region: String,
        _image_url: &str,
        _req_vcpu: i32,
        _req_mem: i64,
        _bandwidth: u64,
    ) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
#[derive(Clone)]
pub struct TestLogger {}

#[cfg(test)]
#[async_trait]
impl LogsProvider for TestLogger {
    async fn new_jobs<'a>(
        &'a self,
        _client: &'a Provider<Ws>,
    ) -> Result<Box<dyn Stream<Item = (H256, bool)> + 'a>> {
        let logs: Vec<Log> = Vec::new();
        Ok(Box::new(
            tokio_stream::iter(
                logs.iter()
                    .map(|job| (job.topics[1], false))
                    .collect::<Vec<_>>(),
            )
            .throttle(Duration::from_secs(2)),
        ))
    }

    async fn job_logs<'a>(
        &'a self,
        _client: &'a Provider<Ws>,
        job: H256,
    ) -> Result<Box<dyn Stream<Item = Log> + Send + 'a>> {
        let logs: Vec<Log> = Vec::new();
        Ok(Box::new(
            tokio_stream::iter(
                logs.into_iter()
                    .filter(|log| log.topics[1] == job)
                    .collect::<Vec<_>>(),
            )
            .throttle(Duration::from_secs(2)),
        ))
    }
}

#[cfg(test)]
#[derive(Clone)]
pub enum Action {
    Open,                // metadata(region, url, instance), rate, balance, timestamp
    Close,               //
    Settle,              // amount, timestamp
    Deposit,             // amount
    Withdraw,            // amount
    ReviseRateInitiated, // new_rate
    ReviseRateCancelled, //
    ReviseRateFinalized, //
}

#[cfg(test)]
pub fn get_rates() -> Option<Vec<RegionalRates>> {
    let file_path = "./rates.json";
    let contents = fs::read_to_string(file_path);

    if let Err(err) = contents {
        println!("Error reading rates file : {err}");
        return None;
    }
    let contents = contents.unwrap();
    let rates: Vec<RegionalRates> = serde_json::from_str(&contents).unwrap_or_default();
    Some(rates)
}

#[cfg(test)]
pub fn get_gb_rates() -> Option<Vec<GBRateCard>> {
    let file_path = "./GB_rates.json";
    let contents = fs::read_to_string(file_path);

    if let Err(err) = contents {
        println!("Error reading rates file : {err}");
        return None;
    }
    let contents = contents.unwrap();
    let rates: Vec<GBRateCard> = serde_json::from_str(&contents).unwrap_or_default();
    Some(rates)
}

#[cfg(test)]
pub fn get_log(topic: Action, data: Bytes, idx: H256) -> Log {
    let mut log = Log {
        address: H160::from_str("0x0F5F91BA30a00bD43Bd19466f020B3E5fc7a49ec").unwrap(),
        removed: Some(false),
        data,
        ..Default::default()
    };
    match topic {
        Action::Open => {
            log.topics = vec![
                H256::from(keccak256(
                    "JobOpened(bytes32,string,address,address,uint256,uint256,uint256)",
                )),
                idx,
                H256::from_low_u64_be(log.address.to_low_u64_be()),
            ];
        }
        Action::Close => {
            log.topics = vec![H256::from(keccak256("JobClosed(bytes32)")), idx];
        }
        Action::Settle => {
            log.topics = vec![
                H256::from(keccak256("JobSettled(bytes32,uint256,uint256)")),
                idx,
            ];
        }
        Action::Deposit => {
            log.topics = vec![
                H256::from(keccak256("JobDeposited(bytes32,address,uint256)")),
                idx,
            ];
        }
        Action::Withdraw => {
            log.topics = vec![
                H256::from(keccak256("JobWithdrew(bytes32,address,uint256)")),
                idx,
            ];
        }
        Action::ReviseRateInitiated => {
            log.topics = vec![
                H256::from(keccak256("JobReviseRateInitiated(bytes32,uint256)")),
                idx,
            ];
        }
        Action::ReviseRateCancelled => {
            log.topics = vec![
                H256::from(keccak256("JobReviseRateCancelled(bytes32)")),
                idx,
            ];
        }
        Action::ReviseRateFinalized => {
            log.topics = vec![
                H256::from(keccak256("JobReviseRateFinalized(bytes32, uint256)")),
                idx,
            ];
        }
    }

    log
}
