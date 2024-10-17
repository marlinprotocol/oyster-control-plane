use std::collections::HashMap;
use std::str::FromStr;

use alloy::hex::ToHexExt;
use alloy::primitives::B128;
use alloy::primitives::{keccak256, Address, Bytes, LogData, B256, B64, U256};
use alloy::providers::Provider;
use alloy::pubsub::PubSubFrontend;
use alloy::rpc::types::eth::Log;
use anyhow::{anyhow, Result};
use tokio::time::{Duration, Instant};
use tokio_stream::StreamExt;

use crate::market::{GBRateCard, InfraProvider, JobId, LogsProvider, RateCard, RegionalRates};

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
    pub instance_id: String,
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub struct SpinDownOutcome {
    pub time: Instant,
    pub job: String,
    pub instance_id: String,
    pub region: String,
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub struct RunEnclaveOutcome {
    pub time: Instant,
    pub job: String,
    pub instance_id: String,
    pub family: String,
    pub region: String,
    pub eif_url: String,
    pub req_mem: i64,
    pub req_vcpu: i32,
    pub bandwidth: u64,
    pub debug: bool,
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub enum TestAwsOutcome {
    SpinUp(SpinUpOutcome),
    SpinDown(SpinDownOutcome),
    RunEnclave(RunEnclaveOutcome),
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
        let instance_id = "i-".to_string() + &instance_id.unwrap_or(B128::random().encode_hex());

        let ip_address = ip_address.unwrap_or(
            B64::random()
                .as_slice()
                .iter()
                .map(|x| x.to_string())
                .reduce(|a, b| a + "." + &b)
                .unwrap(),
        );
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
        job: &JobId,
        instance_type: &str,
        family: &str,
        region: &str,
        req_mem: i64,
        req_vcpu: i32,
        bandwidth: u64,
    ) -> Result<String> {
        let res = self.instances.get_key_value(&job.id);
        if let Some(x) = res {
            self.outcomes.push(TestAwsOutcome::SpinUp(SpinUpOutcome {
                time: Instant::now(),
                job: job.id.clone(),
                instance_type: instance_type.to_owned(),
                family: family.to_owned(),
                region: region.to_owned(),
                req_mem,
                req_vcpu,
                bandwidth,
                eif_url: eif_url.to_owned(),
                contract_address: job.contract.clone(),
                chain_id: job.chain.clone(),
                instance_id: x.1.instance_id.clone(),
            }));

            return Ok(x.1.instance_id.clone());
        }

        let instance_metadata: InstanceMetadata = InstanceMetadata::new(None, None).await;
        self.instances
            .insert(job.id.clone(), instance_metadata.clone());

        self.outcomes.push(TestAwsOutcome::SpinUp(SpinUpOutcome {
            time: Instant::now(),
            job: job.id.clone(),
            instance_type: instance_type.to_owned(),
            family: family.to_owned(),
            region: region.to_owned(),
            req_mem,
            req_vcpu,
            bandwidth,
            eif_url: eif_url.to_owned(),
            contract_address: job.contract.clone(),
            chain_id: job.chain.clone(),
            instance_id: instance_metadata.instance_id.clone(),
        }));

        Ok(instance_metadata.instance_id)
    }

    async fn spin_down(&mut self, instance_id: &str, job: &JobId, region: &str) -> Result<()> {
        self.outcomes
            .push(TestAwsOutcome::SpinDown(SpinDownOutcome {
                time: Instant::now(),
                job: job.id.clone(),
                instance_id: instance_id.to_owned(),
                region: region.to_owned(),
            }));

        self.instances.remove(&job.id);

        Ok(())
    }

    async fn get_job_instance(&self, job: &JobId, _region: &str) -> Result<(bool, String, String)> {
        let res = self.instances.get_key_value(&job.id);
        if let Some(x) = res {
            return Ok((true, x.1.instance_id.clone(), "running".to_owned()));
        }

        Ok((false, String::new(), String::new()))
    }

    async fn get_job_ip(&self, job: &JobId, _region: &str) -> Result<String> {
        let instance_metadata = self.instances.get(&job.id);
        instance_metadata
            .map(|x| x.ip_address.clone())
            .ok_or(anyhow!("Instance not found for job - {}", job.id))
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
        job: &JobId,
        instance_id: &str,
        family: &str,
        region: &str,
        image_url: &str,
        req_vcpu: i32,
        req_mem: i64,
        bandwidth: u64,
        debug: bool,
    ) -> Result<()> {
        self.outcomes
            .push(TestAwsOutcome::RunEnclave(RunEnclaveOutcome {
                time: Instant::now(),
                job: job.id.clone(),
                instance_id: instance_id.to_owned(),
                family: family.to_owned(),
                region: region.to_owned(),
                eif_url: image_url.to_owned(),
                req_mem,
                req_vcpu,
                bandwidth,
                debug,
            }));

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
        let job_id = self.instances.iter().find_map(|(key, val)| {
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
                .find_map(|(i, outcome)| match outcome {
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

            eif_url.clone_into(&mut spin_up_outcome.eif_url);
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
        _client: &'a impl Provider<PubSubFrontend>,
    ) -> Result<impl StreamExt<Item = (B256, bool)> + 'a> {
        let logs: Vec<Log> = Vec::new();
        Ok(tokio_stream::iter(
            logs.iter()
                .map(|job| (job.topics()[1], false))
                .collect::<Vec<_>>(),
        )
        .throttle(Duration::from_secs(2)))
    }

    async fn job_logs<'a>(
        &'a self,
        _client: &'a impl Provider<PubSubFrontend>,
        job: B256,
    ) -> Result<impl StreamExt<Item = Log> + Send + 'a> {
        let logs: Vec<Log> = Vec::new();
        Ok(tokio_stream::iter(
            logs.into_iter()
                .filter(|log| log.topics()[1] == job)
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
            min_rate: U256::from_str_radix("29997916666666", 10).unwrap(),
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
        rate: U256::from_str_radix("109300000000000000", 10).unwrap(),
    }]
}

#[cfg(test)]
pub fn get_log(topic: Action, data: Bytes, idx: B256) -> Log {
    let mut log = Log {
        inner: alloy::primitives::Log {
            address: Address::from_str("0x0F5F91BA30a00bD43Bd19466f020B3E5fc7a49ec").unwrap(),
            data: LogData::new_unchecked(vec![], Bytes::new()),
        },
        ..Default::default()
    };
    match topic {
        Action::Open => {
            log.inner.data = LogData::new_unchecked(
                vec![
                    keccak256("JobOpened(bytes32,string,address,address,uint256,uint256,uint256)"),
                    idx,
                    log.address().into_word(),
                    log.address().into_word(),
                ],
                data,
            );
        }
        Action::Close => {
            log.inner.data =
                LogData::new_unchecked(vec![keccak256("JobClosed(bytes32)"), idx], data);
        }
        Action::Settle => {
            log.inner.data = LogData::new_unchecked(
                vec![keccak256("JobSettled(bytes32,uint256,uint256)"), idx],
                data,
            );
        }
        Action::Deposit => {
            log.inner.data = LogData::new_unchecked(
                vec![
                    keccak256("JobDeposited(bytes32,address,uint256)"),
                    idx,
                    log.address().into_word(),
                ],
                data,
            );
        }
        Action::Withdraw => {
            log.inner.data = LogData::new_unchecked(
                vec![
                    keccak256("JobWithdrew(bytes32,address,uint256)"),
                    idx,
                    log.address().into_word(),
                ],
                data,
            );
        }
        Action::ReviseRateInitiated => {
            log.inner.data = LogData::new_unchecked(
                vec![keccak256("JobReviseRateInitiated(bytes32,uint256)"), idx],
                data,
            );
        }
        Action::ReviseRateCancelled => {
            log.inner.data = LogData::new_unchecked(
                vec![keccak256("JobReviseRateCancelled(bytes32)"), idx],
                data,
            );
        }
        Action::ReviseRateFinalized => {
            log.inner.data = LogData::new_unchecked(
                vec![keccak256("JobReviseRateFinalized(bytes32,uint256)"), idx],
                data,
            );
        }
        Action::MetadataUpdated => {
            log.inner.data = LogData::new_unchecked(
                vec![keccak256("JobMetadataUpdated(bytes32,string)"), idx],
                data,
            );
        }
    }

    log
}
