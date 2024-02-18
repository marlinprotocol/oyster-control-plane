use anyhow::{anyhow, Result};
use ethers::prelude::rand::Rng;
use ethers::prelude::*;
use ethers::types::Log;
use ethers::utils::keccak256;
use std::collections::HashMap;
use std::iter;
use std::str::FromStr;
use tokio::time::{Duration, Instant};
use tokio_stream::{Stream, StreamExt};

use crate::market::{GBRateCard, InfraProvider, LogsProvider, RateCard, RegionalRates};

#[cfg(test)]
#[derive(Clone, Debug)]
pub struct SpinUpOutcome {
    pub time: Instant,
    pub job: String,
    pub instance_type: String,
    pub family: String,
    pub region: String,
    pub req_mem: i64,
    pub req_vcpu: i32,
    pub bandwidth: u64,
    pub eif_url: String,
    pub contract_address: String,
    pub chain_id: String,
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
pub async fn generate_random_string(charset: Option<&[u8]>, len: usize) -> String {
    let charset =
        charset.unwrap_or(b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789");
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
        let instance_id = "i-".to_string()
            + &instance_id.unwrap_or(generate_random_string(Some(b"1234567890abcdef"), 17).await);

        let ip_address = ip_address.unwrap_or(generate_random_ip().await);
        Self {
            instance_id,
            ip_address,
        }
    }
}

#[cfg(test)]
#[derive(Clone, Default)]
pub struct TestAws {
    pub outcomes: Vec<TestAwsOutcome>,

    // HashMap format - (Job, InstanceMetadata)
    pub instances: HashMap<String, InstanceMetadata>,
}

#[cfg(test)]
impl InfraProvider for TestAws {
    async fn spin_up(
        &mut self,
        eif_url: &str,
        job: String,
        instance_type: &str,
        family: &str,
        region: &str,
        req_mem: i64,
        req_vcpu: i32,
        bandwidth: u64,
        contract_address: String,
        chain_id: String,
    ) -> Result<String> {
        self.outcomes.push(TestAwsOutcome::SpinUp(SpinUpOutcome {
            time: Instant::now(),
            job: job.clone(),
            instance_type: instance_type.to_owned(),
            family: family.to_owned(),
            region: region.to_owned(),
            req_mem,
            req_vcpu,
            bandwidth,
            eif_url: eif_url.to_owned(),
            contract_address,
            chain_id,
        }));

        let res = self.instances.get_key_value(&job);
        if let Some(x) = res {
            return Ok(x.1.instance_id.clone());
        }

        let instance_metadata: InstanceMetadata = InstanceMetadata::new(None, None).await;
        self.instances.insert(job, instance_metadata.clone());

        Ok(instance_metadata.instance_id)
    }

    async fn spin_down(&mut self, instance_id: &str, job: String, region: &str) -> Result<()> {
        self.outcomes
            .push(TestAwsOutcome::SpinDown(SpinDownOutcome {
                time: Instant::now(),
                job: job.clone(),
                _instance_id: instance_id.to_owned(),
                region: region.to_owned(),
            }));

        self.instances.remove(&job);

        Ok(())
    }

    async fn get_job_instance(&self, job: &str, _region: &str) -> Result<(bool, String, String)> {
        let res = self.instances.get_key_value(job);
        if let Some(x) = res {
            return Ok((true, x.1.instance_id.clone(), "running".to_owned()));
        }

        Ok((false, String::new(), String::new()))
    }

    async fn get_job_ip(&self, job_id: &str, _region: &str) -> Result<String> {
        let instance_metadata = self.instances.get(job_id);
        if instance_metadata.is_some() {
            return Ok(instance_metadata.unwrap().ip_address.clone());
        }
        return Err(anyhow!("Instance not found for job - {job_id}"));
    }

    async fn check_instance_running(&mut self, _instance_id: &str, _region: &str) -> Result<bool> {
        // println!("TEST: check_instance_running | instance_id: {}, region: {}", instance_id, region);
        Ok(true)
    }

    async fn check_enclave_running(&mut self, _instance_id: &str, _region: &str) -> Result<bool> {
        Ok(true)
    }

    async fn run_enclave(
        &mut self,
        _job: String,
        _instance_id: &str,
        _family: &str,
        _region: &str,
        _image_url: &str,
        _req_vcpu: i32,
        _req_mem: i64,
        _bandwidth: u64,
    ) -> Result<()> {
        Ok(())
    }

    async fn update_enclave_image(
        &mut self,
        instance_id: &str,
        region: &str,
        eif_url: &str,
        req_vcpu: i32,
        req_mem: i64,
    ) -> Result<()> {
        let job_id = self.instances.iter().find_map(|(key, &ref val)| {
            if val.instance_id == instance_id {
                Some(key)
            } else {
                None
            }
        });
        if job_id.is_none() {
            return Err(anyhow!(
                "Instance not found for instance_id - {instance_id}"
            ));
        }
        let job_id = job_id.unwrap();

        let spin_up_outcome_index =
            self.outcomes
                .iter()
                .enumerate()
                .find_map(|(i, &ref outcome)| match outcome {
                    TestAwsOutcome::SpinUp(spin_up) if spin_up.job == instance_id => Some(i),
                    _ => None,
                });

        if spin_up_outcome_index.is_none() {
            return Err(anyhow!("Spin up outcome not found for job - {}", job_id));
        }

        let spin_up_outcome_index = spin_up_outcome_index.unwrap();

        if let TestAwsOutcome::SpinUp(spin_up_outcome) = &mut self.outcomes[spin_up_outcome_index] {
            if spin_up_outcome.region != region
                || spin_up_outcome.req_vcpu != req_vcpu
                || spin_up_outcome.req_mem != req_mem
            {
                return Err(anyhow!("Can only change EIF URL"));
            }

            if spin_up_outcome.eif_url == eif_url {
                return Err(anyhow!("Must input a different EIF URL"));
            }

            spin_up_outcome.eif_url = eif_url.to_owned();
        } else {
            panic!("Spin up outcome not found at proper index for the job.")
        }

        Ok(())
    }
}

#[cfg(test)]
#[derive(Clone)]
pub struct TestLogger {}

#[cfg(test)]
impl LogsProvider for TestLogger {
    async fn new_jobs<'a>(
        &'a self,
        _client: &'a Provider<Ws>,
    ) -> Result<impl Stream<Item = (H256, bool)> + 'a> {
        let logs: Vec<Log> = Vec::new();
        Ok(tokio_stream::iter(
            logs.iter()
                .map(|job| (job.topics[1], false))
                .collect::<Vec<_>>(),
        )
        .throttle(Duration::from_secs(2)))
    }

    async fn job_logs<'a>(
        &'a self,
        _client: &'a Provider<Ws>,
        job: H256,
    ) -> Result<impl Stream<Item = Log> + Send + 'a> {
        let logs: Vec<Log> = Vec::new();
        Ok(tokio_stream::iter(
            logs.into_iter()
                .filter(|log| log.topics[1] == job)
                .collect::<Vec<_>>(),
        )
        .throttle(Duration::from_secs(2)))
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
    MetadataUpdated,
}

#[cfg(test)]
pub fn get_rates() -> Vec<RegionalRates> {
    vec![RegionalRates {
        region: "ap-south-1".to_owned(),
        rate_cards: vec![RateCard {
            instance: "c6a.xlarge".to_owned(),
            min_rate: U256::from_dec_str("29997916666666").unwrap(),
            cpu: 4,
            memory: 8,
            arch: String::from("amd64"),
        }],
    }]
}

#[cfg(test)]
pub fn get_gb_rates() -> Vec<GBRateCard> {
    vec![GBRateCard {
        region: "Asia South (Mumbai)".to_owned(),
        region_code: "ap-south-1".to_owned(),
        rate: U256::from_dec_str("109300000000000000").unwrap(),
    }]
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
        Action::MetadataUpdated => {
            log.topics = vec![
                H256::from(keccak256("JobMetadataUpdated(bytes32,string)")),
                idx,
            ];
        }
    }

    log
}
