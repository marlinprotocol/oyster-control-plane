use anyhow::Result;
use async_trait::async_trait;
use ethers::prelude::rand::Rng;
use ethers::prelude::*;
use ethers::types::Log;
use ethers::utils::keccak256;
use std::collections::HashMap;
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
#[derive(Clone, Default)]
pub struct TestAws {
    pub outcomes: Vec<TestAwsOutcome>,
    pub instances: HashMap<String, String>,
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
        contract_address: String,
        chain_id: String,
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
            contract_address,
            chain_id,
        }));

        let res = self.instances.get_key_value(&job);
        if let Some(x) = res {
            return Ok(x.1.clone());
        }

        let id: String = rand::thread_rng()
            .sample_iter(rand::distributions::Alphanumeric)
            .take(8)
            .map(char::from)
            .collect();
        self.instances.insert(job, id.clone());

        Ok(id)
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
            return Ok((true, x.1.clone(), "running".to_owned()));
        }

        Ok((false, String::new(), String::new()))
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
    }

    log
}
