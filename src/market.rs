use async_trait::async_trait;
use ethers::abi::{AbiDecode, AbiEncode};
use ethers::prelude::*;
use ethers::types::serde_helpers::deserialize_stringified_numeric;
use ethers::utils::keccak256;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use anyhow::{Context, Result};
use tokio::time::sleep;
use tokio::time::{Duration, Instant};
use tokio_stream::Stream;

use ethers::types::Log;

use crate::aws::PRC;

// IMPORTANT: do not import SystemTime, use the now_timestamp helper

// Basic architecture:
// One future listening to new jobs
// Each job has its own future managing its lifetime

#[async_trait]
pub trait InfraProvider {
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
    ) -> Result<String>;

    async fn spin_down(&mut self, instance_id: &str, job: String, region: String) -> Result<bool>;

    async fn get_job_instance(&self, job: &str, region: String) -> Result<(bool, String, String)>;

    async fn get_job_ip(&self, job: &str, region: String) -> Result<String>;

    async fn check_instance_running(&mut self, instance_id: &str, region: String) -> Result<bool>;

    async fn get_job_enclave_state(&self, job: &str, region: String) -> Result<(String, PRC)>;

    async fn check_enclave_running(&mut self, instance_id: &str, region: String) -> Result<bool>;

    async fn run_enclave(
        &mut self,
        job: String,
        instance_id: &str,
        region: String,
        image_url: &str,
        req_vcpu: i32,
        req_mem: i64,
        bandwidth: u64,
    ) -> Result<()>;
}

#[async_trait]
impl<'a, T> InfraProvider for &'a mut T
where
    T: InfraProvider + Send + Sync,
{
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
        (**self)
            .spin_up(
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
    }

    async fn spin_down(&mut self, instance_id: &str, job: String, region: String) -> Result<bool> {
        (**self).spin_down(instance_id, job, region).await
    }

    async fn get_job_instance(&self, job: &str, region: String) -> Result<(bool, String, String)> {
        (**self).get_job_instance(job, region).await
    }

    async fn get_job_ip(&self, job: &str, region: String) -> Result<String> {
        (**self).get_job_ip(job, region).await
    }

    async fn check_instance_running(&mut self, instance_id: &str, region: String) -> Result<bool> {
        (**self).check_instance_running(instance_id, region).await
    }

    async fn get_job_enclave_state(&self, job: &str, region: String) -> Result<(String, PRC)> {
        (**self).get_job_enclave_state(job, region).await
    }

    async fn check_enclave_running(&mut self, instance_id: &str, region: String) -> Result<bool> {
        (**self).check_enclave_running(instance_id, region).await
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
    ) -> Result<()> {
        (**self)
            .run_enclave(
                job,
                instance_id,
                region,
                image_url,
                req_vcpu,
                req_mem,
                bandwidth,
            )
            .await
    }
}

#[async_trait]
pub trait LogsProvider {
    async fn new_jobs<'a>(
        &'a self,
        client: &'a Provider<Ws>,
    ) -> Result<Box<dyn Stream<Item = (H256, bool)> + 'a>>;

    async fn job_logs<'a>(
        &'a self,
        client: &'a Provider<Ws>,
        job: H256,
    ) -> Result<Box<dyn Stream<Item = Log> + Send + 'a>>;
}

#[derive(Clone)]
pub struct EthersProvider {
    pub contract: Address,
    pub provider: Address,
}

#[async_trait]
impl LogsProvider for EthersProvider {
    async fn new_jobs<'a>(
        &'a self,
        client: &'a Provider<Ws>,
    ) -> Result<Box<dyn Stream<Item = (H256, bool)> + 'a>> {
        new_jobs(client, self.contract, self.provider).await
    }

    async fn job_logs<'a>(
        &'a self,
        client: &'a Provider<Ws>,
        job: H256,
    ) -> Result<Box<dyn Stream<Item = Log> + Send + 'a>> {
        job_logs(client, self.contract, job).await
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct RateCard {
    pub instance: String,
    #[serde(deserialize_with = "deserialize_stringified_numeric")]
    pub min_rate: U256,
    pub cpu: u32,
    pub memory: u32,
    pub arch: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct RegionalRates {
    pub region: String,
    pub rate_cards: Vec<RateCard>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct GBRateCard {
    pub region: String,
    pub region_code: String,
    #[serde(deserialize_with = "deserialize_stringified_numeric")]
    pub rate: U256,
}

pub async fn run(
    infra_provider: impl InfraProvider + Send + Sync + Clone + 'static,
    logs_provider: impl LogsProvider + Send + Sync + Clone + 'static,
    url: String,
    regions: Vec<String>,
    rates: &'static [RegionalRates],
    gb_rates: &'static [GBRateCard],
    address_whitelist: &'static [String],
    address_blacklist: &'static [String],
    contract_address: String,
    chain_id: String,
) {
    let mut backoff = 1;

    // connection level loop
    // start from scratch in case of connection errors
    // trying to implicitly resume connections or event streams can cause issues
    // since subscriptions are stateful

    loop {
        println!("main: Connecting to RPC endpoint...");
        let res = Provider::<Ws>::connect(url.clone()).await;
        if let Err(err) = res {
            // exponential backoff on connection errors
            println!("main: Connection error: {err:?}");
            sleep(Duration::from_secs(backoff)).await;
            backoff *= 2;
            if backoff > 128 {
                backoff = 128;
            }
            continue;
        }
        backoff = 1;
        println!("main: Connected to RPC endpoint");

        let client = res.unwrap();
        let res = logs_provider.new_jobs(&client).await;
        if let Err(err) = res {
            println!("main: Subscribe error: {err:?}");
            sleep(Duration::from_secs(1)).await;
            continue;
        }

        let job_stream = Box::into_pin(res.unwrap());
        run_once(
            job_stream,
            infra_provider.clone(),
            logs_provider.clone(),
            url.clone(),
            regions.clone(),
            rates,
            gb_rates,
            address_whitelist,
            address_blacklist,
            contract_address.clone(),
            chain_id.clone(),
        )
        .await;
    }
}

async fn run_once(
    mut job_stream: impl Stream<Item = (H256, bool)> + Unpin,
    infra_provider: impl InfraProvider + Send + Sync + Clone + 'static,
    logs_provider: impl LogsProvider + Send + Sync + Clone + 'static,
    url: String,
    regions: Vec<String>,
    rates: &'static [RegionalRates],
    gb_rates: &'static [GBRateCard],
    address_whitelist: &'static [String],
    address_blacklist: &'static [String],
    contract_address: String,
    chain_id: String,
) {
    while let Some((job, removed)) = job_stream.next().await {
        println!("main: New job: {job}, {removed}");
        tokio::spawn(job_manager(
            infra_provider.clone(),
            logs_provider.clone(),
            url.clone(),
            job,
            regions.clone(),
            3,
            rates,
            gb_rates,
            address_whitelist,
            address_blacklist,
            contract_address.clone(),
            chain_id.clone(),
        ));
    }

    println!("main: Job stream ended");
}

async fn new_jobs(
    client: &Provider<Ws>,
    address: Address,
    provider: Address,
) -> Result<Box<dyn Stream<Item = (H256, bool)> + '_>> {
    let event_filter = Filter::new()
        .address(address)
        .select(0..)
        .topic0(vec![keccak256(
            "JobOpened(bytes32,string,address,address,uint256,uint256,uint256)",
        )])
        .topic3(provider);

    // register subscription
    let stream = client
        .subscribe_logs(&event_filter)
        .await
        .context("failed to subscribe to new jobs")?;

    Ok(Box::new(stream.map(|item| {
        (item.topics[1], item.removed.unwrap_or(false))
    })))
}

// manage the complete lifecycle of a job
async fn job_manager(
    infra_provider: impl InfraProvider + Send + Sync + Clone,
    logs_provider: impl LogsProvider + Send + Sync,
    url: String,
    job: H256,
    allowed_regions: Vec<String>,
    aws_delay_duration: u64,
    rates: &[RegionalRates],
    gb_rates: &[GBRateCard],
    address_whitelist: &[String],
    address_blacklist: &[String],
    contract_address: String,
    chain_id: String,
) {
    let mut backoff = 1;

    // connection level loop
    // start from scratch in case of connection errors
    // trying to implicitly resume connections or event streams can cause issues
    // since subscriptions are stateful
    loop {
        println!("job {job}: Connecting to RPC endpoint...");
        let res = Provider::<Ws>::connect(url.clone()).await;
        if let Err(err) = res {
            // exponential backoff on connection errors
            println!("job {job}: Connection error: {err:?}");
            sleep(Duration::from_secs(backoff)).await;
            backoff *= 2;
            if backoff > 128 {
                backoff = 128;
            }
            continue;
        }
        backoff = 1;
        println!("job {job}: Connected to RPC endpoint");

        let client = res.unwrap();
        let res = logs_provider.job_logs(&client, job).await;
        if let Err(err) = res {
            println!("job {job}: Subscribe error: {err:?}");
            sleep(Duration::from_secs(1)).await;
            continue;
        }

        let job_stream = Box::into_pin(res.unwrap());
        let res = job_manager_once(
            job_stream,
            infra_provider.clone(),
            job,
            allowed_regions.clone(),
            aws_delay_duration,
            rates,
            gb_rates,
            address_whitelist,
            address_blacklist,
            contract_address.clone(),
            chain_id.clone(),
        )
        .await;

        if res == -2 || res == 0 {
            // full exit
            break;
        }
    }
}

fn whitelist_blacklist_check(
    log: Log,
    address_whitelist: &[String],
    address_blacklist: &[String],
) -> bool {
    // check whitelist
    if !address_whitelist.is_empty() {
        println!("Checking address whitelist...");
        if address_whitelist
            .iter()
            .any(|s| s == &log.topics[2].encode_hex())
        {
            println!("ADDRESS ALLOWED!");
        } else {
            println!("ADDRESS NOT ALLOWED!");
            return false;
        }
    }

    // check blacklist
    if !address_blacklist.is_empty() {
        println!("Checking address blacklist...");
        if address_blacklist
            .iter()
            .any(|s| s == &log.topics[2].encode_hex())
        {
            println!("ADDRESS NOT ALLOWED!");
            return false;
        } else {
            println!("ADDRESS ALLOWED!");
        }
    }

    true
}
struct JobState {
    job: H256,
    launch_delay: u64,
    allowed_regions: Vec<String>,
    contract_address: String,
    chain_id: String,

    balance: U256,
    last_settled: Duration,
    rate: U256,
    original_rate: U256,
    instance_id: String,
    min_rate: U256,
    bandwidth: u64,
    eif_url: String,
    instance_type: String,
    region: String,
    req_vcpus: i32,
    req_mem: i64,

    // whether instance should exist or not
    infra_state: bool,
    // how long to wait for infra change
    infra_change_time: Instant,
    // whether to schedule change
    infra_change_scheduled: bool,
}

impl JobState {
    fn new(
        job: H256,
        launch_delay: u64,
        allowed_regions: Vec<String>,
        contract_address: String,
        chain_id: String,
    ) -> JobState {
        // solvency metrics
        // default of 60s
        JobState {
            job,
            launch_delay,
            allowed_regions,
            contract_address,
            chain_id,
            balance: U256::from(360),
            last_settled: now_timestamp(),
            rate: U256::one(),
            original_rate: U256::one(),
            instance_id: String::new(),
            min_rate: U256::MAX,
            bandwidth: 0,
            eif_url: String::new(),
            instance_type: "c6a.xlarge".to_string(),
            region: "ap-south-1".to_string(),
            req_vcpus: 2,
            req_mem: 4096,
            infra_state: false,
            infra_change_time: Instant::now(),
            infra_change_scheduled: false,
        }
    }

    fn insolvency_duration(&self) -> Duration {
        let now_ts = now_timestamp();
        let sat_convert = |n: U256| n.clamp(U256::zero(), u64::MAX.into()).low_u64();

        if self.rate == U256::zero() {
            Duration::from_secs(0)
        } else {
            // solvent for balance / rate seconds from last_settled with 300s as margin
            Duration::from_secs(
                sat_convert(self.balance * U256::exp10(12) / self.rate).saturating_sub(300),
            )
            .saturating_sub(now_ts.saturating_sub(self.last_settled))
        }
    }

    async fn heartbeat_check(&mut self, mut infra_provider: impl InfraProvider) {
        // TODO: should check if enclave is running as well
        let job = &self.job;
        let is_running = infra_provider
            .check_instance_running(&self.instance_id, self.region.clone())
            .await;
        match is_running {
            Err(err) => {
                println!("job {job}: failed to retrieve instance state, {err:?}");
            }
            Ok(is_running) => {
                if is_running {
                    let is_enclave_running = infra_provider
                        .check_enclave_running(&self.instance_id, self.region.clone())
                        .await;

                    match is_enclave_running {
                        Ok(is_enclave_running) => {
                            if !is_enclave_running {
                                println!("job {job}: enclave not running on the instance, running the enclave");
                                let res = infra_provider
                                    .run_enclave(
                                        job.encode_hex(),
                                        &self.instance_id,
                                        self.region.clone(),
                                        &self.eif_url,
                                        self.req_vcpus,
                                        self.req_mem,
                                        self.bandwidth,
                                    )
                                    .await;
                                match res {
                                    Ok(_) => {
                                        println!(
                                            "job {job}: enclave successfully ran on the instance"
                                        );
                                    }
                                    Err(err) => {
                                        println!("job {job}: failed to run enclave, {err:?}. Spinning instance down.");
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            println!("job {job}: failed to retrieve enclave state, {err:?}");
                        }
                    }
                } else if !is_running && self.rate >= self.min_rate {
                    println!("job {job}: instance not running, scheduling new launch");
                    self.schedule_launch(0);
                }
            }
        }
    }

    fn handle_insolvency(&mut self) {
        let job = &self.job;
        println!("job {job}: INSOLVENCY");
        self.schedule_termination(0);
    }

    fn schedule_launch(&mut self, delay: u64) {
        let job = &self.job;
        self.infra_change_scheduled = true;
        self.infra_change_time = Instant::now()
            .checked_add(Duration::from_secs(delay))
            .unwrap();
        self.infra_state = true;
        println!("job {job}: Instance launch scheduled");
    }

    fn schedule_termination(&mut self, delay: u64) {
        let job = &self.job;
        self.infra_change_scheduled = true;
        self.infra_change_time = Instant::now()
            .checked_add(Duration::from_secs(delay))
            .unwrap();
        self.infra_state = false;
        println!("job {job}: Instance termination scheduled");
    }

    async fn change_infra(&mut self, infra_provider: impl InfraProvider) -> bool {
        let res = self.change_infra_impl(infra_provider).await;
        if res {
            // successful
            self.infra_change_scheduled = false;
        } else {
            // failed, reschedule with small delay
            self.infra_change_time = Instant::now() + Duration::from_secs(2);
        }

        res
    }

    async fn change_infra_impl(&mut self, mut infra_provider: impl InfraProvider) -> bool {
        let job = &self.job;

        let res = infra_provider
            .get_job_instance(&job.encode_hex(), self.region.clone())
            .await;

        if let Err(err) = res {
            println!("job {job}: ERROR failed to get job instance, {err:?}");
            return false;
        }
        let (exist, instance, state) = res.unwrap();

        if self.infra_state {
            // launch mode
            if exist {
                // instance exists already
                if state == "pending" || state == "running" {
                    // instance exists and is already running, we are done
                    println!("job {job}: found existing healthy instance: {instance}");
                    self.instance_id = instance;
                    return true;
                }

                if state == "stopping" || state == "stopped" {
                    // instance unhealthy, terminate
                    println!("job {job}: found existing unhealthy instance: {instance}");
                    let res = infra_provider
                        .spin_down(&instance, job.encode_hex(), self.region.clone())
                        .await;
                    if let Err(err) = res {
                        println!("job {job}: ERROR failed to terminate instance, {err:?}");
                        return false;
                    }
                }

                // state is shutting-down or terminated at this point
            }

            // either no old instance or old instance was not enough, launch new one
            println!("job {job}: launching new instance");
            let res = infra_provider
                .spin_up(
                    self.eif_url.as_str(),
                    job.encode_hex(),
                    self.instance_type.as_str(),
                    self.region.clone(),
                    self.req_mem,
                    self.req_vcpus,
                    self.bandwidth,
                    self.contract_address.clone(),
                    self.chain_id.clone(),
                )
                .await;
            if let Err(err) = res {
                println!("job {job}: Instance launch failed, {err:?}");
                return false;
            }
            self.instance_id = res.unwrap();
            println!("job {job}: Instance launched: {}", self.instance_id);
        } else {
            // terminate mode
            if !exist || state == "shutting-down" || state == "terminated" {
                // instance does not really exist anyway, we are done
                println!("job {job}: instance does not exist or is already terminated");
                return true;
            }

            // terminate instance
            println!("job {job}: terminating existing instance: {instance}");
            let res = infra_provider
                .spin_down(&instance, job.encode_hex(), self.region.clone())
                .await;
            if let Err(err) = res {
                println!("job {job}: ERROR failed to terminate instance, {err:?}");
                return false;
            }
        }

        true
    }

    // return 0 on success
    // -1 on recoverable errors (can retry)
    // -2 on unrecoverable errors (no point retrying)
    // -3 when blacklist/whitelist check fails
    fn process_log(
        &mut self,
        log: Option<Log>,
        rates: &[RegionalRates],
        gb_rates: &[GBRateCard],
        address_whitelist: &[String],
        address_blacklist: &[String],
    ) -> i8 {
        let job = self.job;

        if log.is_none() {
            // error in the stream, can retry with new conn
            return -1;
        }

        let log = log.unwrap();
        println!("job {}: New log: {}, {}", job, log.topics[0], log.data);

        // events
        #[allow(non_snake_case)]
        let JOB_OPENED =
            keccak256("JobOpened(bytes32,string,address,address,uint256,uint256,uint256)").into();
        #[allow(non_snake_case)]
        let JOB_SETTLED = keccak256("JobSettled(bytes32,uint256,uint256)").into();
        #[allow(non_snake_case)]
        let JOB_CLOSED = keccak256("JobClosed(bytes32)").into();
        #[allow(non_snake_case)]
        let JOB_DEPOSITED = keccak256("JobDeposited(bytes32,address,uint256)").into();
        #[allow(non_snake_case)]
        let JOB_WITHDREW = keccak256("JobWithdrew(bytes32,address,uint256)").into();
        #[allow(non_snake_case)]
        let JOB_REVISE_RATE_INITIATED = keccak256("JobReviseRateInitiated(bytes32,uint256)").into();
        #[allow(non_snake_case)]
        let JOB_REVISE_RATE_CANCELLED = keccak256("JobReviseRateCancelled(bytes32)").into();
        #[allow(non_snake_case)]
        let JOB_REVISE_RATE_FINALIZED =
            keccak256("JobReviseRateFinalized(bytes32, uint256)").into();

        // NOTE: jobs should be killed fully if any individual event would kill it
        // regardless of future events
        // helps preserve consistency on restarts where events are procesed all at once
        // e.g. do not spin up if job goes below min_rate and then goes above min_rate

        if log.topics[0] == JOB_OPENED {
            // decode
            if let Ok((metadata, _rate, _balance, timestamp)) =
                <(String, U256, U256, U256)>::decode(&log.data)
            {
                println!(
                    "job {job}: OPENED: metadata: {metadata}, rate: {_rate}, balance: {_balance}, timestamp: {timestamp}, {}",
                    self.last_settled.as_secs()
                );

                // update solvency metrics
                self.balance = _balance;
                self.rate = _rate;
                self.original_rate = _rate;
                self.last_settled = Duration::from_secs(timestamp.low_u64());

                let v = serde_json::from_str(&metadata);
                if let Err(err) = v {
                    println!("job {job}: Error reading metadata: {err:?}");
                    return -2;
                }

                let v: Value = v.unwrap();

                let r = v["instance"].as_str();
                match r {
                    Some(t) => {
                        self.instance_type = t.to_string();
                        println!("job {job}: Instance type set: {}", self.instance_type);
                    }
                    None => {
                        println!("job {job}: Instance type not set");
                        return -2;
                    }
                }

                let r = v["region"].as_str();
                match r {
                    Some(t) => {
                        self.region = t.to_string();
                        println!("job {job}: Job region set: {}", self.region);
                    }
                    None => {
                        println!("job {job}: Job region not set");
                        return -2;
                    }
                }

                if !self.allowed_regions.contains(&self.region) {
                    println!(
                        "job {job}: region: {} not suppported, exiting job",
                        self.region
                    );
                    return -2;
                }

                let r = v["memory"].as_i64();
                match r {
                    Some(t) => {
                        self.req_mem = t;
                        println!("job {job}: Required memory: {}", self.req_mem);
                    }
                    None => {
                        println!("job {job}: memory not set");
                        return -2;
                    }
                }

                let r = v["vcpu"].as_i64();
                match r {
                    Some(t) => {
                        self.req_vcpus = t.try_into().unwrap_or(2);
                        println!("job {job}: Required vcpu: {}", self.req_vcpus);
                    }
                    None => {
                        println!("job {job}: vcpu not set");
                        return -2;
                    }
                }

                let url = v["url"].as_str();
                if url.is_none() {
                    println!("job {job}: eif url not found! Exiting job");
                    return -2;
                }
                self.eif_url = url.unwrap().to_string();

                // blacklist whitelist check
                let allowed =
                    whitelist_blacklist_check(log.clone(), address_whitelist, address_blacklist);
                if !allowed {
                    // blacklisted or not whitelisted address
                    self.schedule_termination(0);
                    return -3;
                }

                let mut supported = false;
                for entry in rates {
                    if entry.region == self.region {
                        for card in &entry.rate_cards {
                            if card.instance == self.instance_type {
                                self.min_rate = card.min_rate;
                                supported = true;
                                break;
                            }
                        }
                        break;
                    }
                }

                if !supported {
                    println!(
                        "job {job}: instance type {}, not supported",
                        self.instance_type
                    );
                    return -2;
                }

                println!(
                    "job {job}: MIN RATE for {} instance is {}",
                    self.instance_type, self.min_rate
                );

                // launch only if rate is more than min
                if self.rate >= self.min_rate {
                    for entry in gb_rates {
                        if entry.region_code == self.region {
                            let gb_cost = entry.rate;
                            let bandwidth_rate = self.rate - self.min_rate;

                            self.bandwidth = ((bandwidth_rate * 1024 * 1024 * 8 / gb_cost) as U256)
                                .clamp(U256::zero(), u64::MAX.into())
                                .low_u64();
                            break;
                        }
                    }
                    self.schedule_launch(self.launch_delay);
                } else {
                    self.schedule_termination(0);
                }
            } else {
                println!("job {job}: OPENED: Decode failure: {}", log.data);
            }
        } else if log.topics[0] == JOB_SETTLED {
            // decode
            if let Ok((amount, timestamp)) = <(U256, U256)>::decode(&log.data) {
                println!(
                    "job {job}: SETTLED: amount: {amount}, rate: {}, balance: {}, timestamp: {}",
                    self.rate,
                    self.balance,
                    self.last_settled.as_secs()
                );
                // update solvency metrics
                self.balance -= amount;
                self.last_settled = Duration::from_secs(timestamp.low_u64());
                println!(
                    "job {job}: SETTLED: amount: {amount}, rate: {}, balance: {}, timestamp: {}",
                    self.rate,
                    self.balance,
                    self.last_settled.as_secs()
                );
            } else {
                println!("job {job}: SETTLED: Decode failure: {}", log.data);
            }
        } else if log.topics[0] == JOB_CLOSED {
            self.schedule_termination(0);
        } else if log.topics[0] == JOB_DEPOSITED {
            // decode
            if let Ok(amount) = U256::decode(&log.data) {
                // update solvency metrics
                println!(
                    "job {job}: DEPOSITED: amount: {amount}, rate: {}, balance: {}, timestamp: {}",
                    self.rate,
                    self.balance,
                    self.last_settled.as_secs()
                );
                self.balance += amount;
                println!(
                    "job {job}: DEPOSITED: amount: {amount}, rate: {}, balance: {}, timestamp: {}",
                    self.rate,
                    self.balance,
                    self.last_settled.as_secs()
                );
            } else {
                println!("job {job}: DEPOSITED: Decode failure: {}", log.data);
            }
        } else if log.topics[0] == JOB_WITHDREW {
            // decode
            if let Ok(amount) = U256::decode(&log.data) {
                println!(
                    "job {job}: WITHDREW: amount: {amount}, rate: {}, balance: {}, timestamp: {}",
                    self.rate,
                    self.balance,
                    self.last_settled.as_secs()
                );
                // update solvency metrics
                self.balance -= amount;
                println!(
                    "job {job}: WITHDREW: amount: {amount}, rate: {}, balance: {}, timestamp: {}",
                    self.rate,
                    self.balance,
                    self.last_settled.as_secs()
                );
            } else {
                println!("job {job}: WITHDREW: Decode failure: {}", log.data);
            }
        } else if log.topics[0] == JOB_REVISE_RATE_INITIATED {
            if let Ok(new_rate) = U256::decode(&log.data) {
                println!(
                    "job {job}: JOB_REVISE_RATE_INTIATED: original_rate: {}, rate: {}, balance: {}, timestamp: {}", 
                    self.original_rate,
                    self.rate,
                    self.balance,
                    self.last_settled.as_secs()
                );
                self.original_rate = self.rate;
                self.rate = new_rate;
                if self.rate < self.min_rate {
                    self.schedule_termination(0);
                    println!("job {job}: Revised job rate below min rate, shut down");
                }
                println!(
                    "job {job}: JOB_REVISE_RATE_INTIATED: original_rate: {}, rate: {}, balance: {}, timestamp: {}", 
                    self.original_rate,
                    self.rate,
                    self.balance,
                    self.last_settled.as_secs()
                );
            } else {
                println!(
                    "job {job}: JOB_REVISE_RATE_INITIATED: Decode failure: {}",
                    log.data
                );
            }
        } else if log.topics[0] == JOB_REVISE_RATE_CANCELLED {
            println!(
                "job {job}: JOB_REVISED_RATE_CANCELLED: rate: {}, balance: {}, timestamp: {}",
                self.rate,
                self.balance,
                self.last_settled.as_secs()
            );
            self.rate = self.original_rate;
            println!(
                "job {job}: JOB_REVISED_RATE_CANCELLED: rate: {}, balance: {}, timestamp: {}",
                self.rate,
                self.balance,
                self.last_settled.as_secs()
            );
        } else if log.topics[0] == JOB_REVISE_RATE_FINALIZED {
            if let Ok(new_rate) = U256::decode(&log.data) {
                println!(
                    "job {job}: JOB_REVISE_RATE_FINALIZED: original_rate: {}, rate: {}, balance: {}, timestamp: {}", 
                    self.original_rate,
                    self.rate,
                    self.balance,
                    self.last_settled.as_secs()
                );
                if self.rate != new_rate {
                    println!("Job {job}: Something went wrong, finalized rate not same as initiated rate");
                    return -2;
                }
                self.original_rate = new_rate;
                println!(
                    "job {job}: JOB_REVISE_RATE_FINALIZED: original_rate: {}, rate: {}, balance: {}, timestamp: {}", 
                    self.original_rate,
                    self.rate,
                    self.balance,
                    self.last_settled.as_secs()
                );
            } else {
                println!(
                    "job {job}: JOB_REVISE_RATE_FINALIZED: Decode failure: {}",
                    log.data
                );
            }
        } else {
            println!("job {job}: Unknown event: {}", log.topics[0]);
        }

        0
    }
}

// manage the complete lifecycle of a job
// returns true if "done"
async fn job_manager_once(
    mut job_stream: impl Stream<Item = Log> + Unpin,
    mut infra_provider: impl InfraProvider + Send + Sync,
    job: H256,
    allowed_regions: Vec<String>,
    aws_delay_duration: u64,
    rates: &[RegionalRates],
    gb_rates: &[GBRateCard],
    address_whitelist: &[String],
    address_blacklist: &[String],
    contract_address: String,
    chain_id: String,
) -> i8 {
    let mut state = JobState::new(
        job,
        aws_delay_duration,
        allowed_regions,
        contract_address,
        chain_id,
    );

    let res = 'event: loop {
        // compute time to insolvency
        let insolvency_duration = state.insolvency_duration();
        println!(
            "job {}: Insolvency after: {}",
            job,
            insolvency_duration.as_secs()
        );

        let aws_delay_timeout = state
            .infra_change_time
            .saturating_duration_since(Instant::now());

        // NOTE: some stuff like cargo fmt does not work inside this macro
        // extract as much stuff as possible outside it
        tokio::select! {
            // order matters
            biased;

            log = job_stream.next() => {
                let res = state.process_log(log, rates, gb_rates, address_whitelist, address_blacklist);
                if res == -2 || res == -1 {
                    break 'event res;
                }
            }

            // running instance heartbeat check
            // should only happen if instance id is available
            () = sleep(Duration::from_secs(5)), if !state.instance_id.is_empty() => {
                state.heartbeat_check(&mut infra_provider).await;
            }

            // insolvency check
            // enable when termination is not already scheduled
            () = sleep(insolvency_duration), if !state.infra_change_scheduled || state.infra_state => {
                state.handle_insolvency();
            }

            // aws delayed spin up check
            // should only happen if scheduled
            () = sleep(aws_delay_timeout), if state.infra_change_scheduled => {
                let res = state.change_infra(&mut infra_provider).await;
                if res && !state.infra_state {
                    // successful termination, exit
                    break 'event 0;
                }
            }
        }
    };

    println!("job {job}: Job stream ended: {res}");

    res
}

async fn job_logs(
    client: &Provider<Ws>,
    contract: Address,
    job: H256,
) -> Result<Box<dyn Stream<Item = Log> + Send + '_>> {
    // TODO: Filter by contract and job
    let event_filter = Filter::new()
        .select(0..)
        .address(contract)
        .topic0(vec![
            keccak256("JobOpened(bytes32,string,address,address,uint256,uint256,uint256)"),
            keccak256("JobSettled(bytes32,uint256,uint256)"),
            keccak256("JobClosed(bytes32)"),
            keccak256("JobDeposited(bytes32,address,uint256)"),
            keccak256("JobWithdrew(bytes32,address,uint256)"),
            keccak256("JobReviseRateInitiated(bytes32,uint256)"),
            keccak256("JobReviseRateCancelled(bytes32)"),
            keccak256("JobReviseRateFinalized(bytes32,uint256)"),
        ])
        .topic1(job);

    // register subscription
    let stream = client
        .subscribe_logs(&event_filter)
        .await
        .context("failed to subscribe to job logs")?;

    Ok(Box::new(stream))
}

#[cfg(not(test))]
fn now_timestamp() -> Duration {
    // import here to ensure it is used only through this function
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
}

#[cfg(test)]
static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

#[cfg(test)]
fn now_timestamp() -> Duration {
    Instant::now() - *START.get().unwrap()
}

// --------------------------------------------------------------------------------------------------------------------------------------------------------
//                                                                  TESTS
// --------------------------------------------------------------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use ethers::abi::AbiEncode;
    use ethers::prelude::*;
    use std::str::FromStr;
    use tokio::time::{sleep, Duration, Instant};

    use crate::market;
    use crate::test::{self, Action, TestAws, TestAwsOutcome};

    #[tokio::test(start_paused = true)]
    async fn test_instance_launch_after_delay_on_spin_up() {
        let _ = market::START.set(Instant::now());

        let job_num = H256::from_low_u64_be(1);
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).encode()),
            (301, Action::Close, [].into()),
        ].into_iter().map(|x| (x.0, test::get_log(x.1, Bytes::from(x.2), job_num))).collect();

        let start_time = Instant::now();
        // pending stream appended so job stream never ends
        let job_stream = std::pin::pin!(tokio_stream::iter(job_logs.into_iter())
            .then(|(moment, log)| async move {
                let delay = start_time + Duration::from_secs(moment) - Instant::now();
                sleep(delay).await;
                log
            })
            .chain(tokio_stream::pending()));
        let mut aws: TestAws = Default::default();
        let res = market::job_manager_once(
            job_stream,
            &mut aws,
            job_num,
            vec!["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::new(),
            "xyz".into(),
            "123".into(),
        )
        .await;

        // job manager should have finished successfully
        assert_eq!(res, 0);
        println!("{:?}", aws.outcomes);
        let spin_up_tv_sec: Instant;
        if let TestAwsOutcome::SpinUp(out) = &aws.outcomes[0] {
            spin_up_tv_sec = out.time;
            assert!(
                H256::from_str(&out.job).unwrap() == job_num
                    && out.instance_type == "c6a.xlarge"
                    && out.region == "ap-south-1"
                    && out.req_mem == 4096
                    && out.req_vcpu == 2
                    && out.bandwidth == 76
                    && out.eif_url == "https://example.com/enclave.eif"
                    && out.contract_address == "xyz"
                    && out.chain_id == "123"
            )
        } else {
            panic!();
        };

        if let TestAwsOutcome::SpinDown(out) = &aws.outcomes[1] {
            assert_eq!((out.time - spin_up_tv_sec).as_secs(), 1);
            assert!(H256::from_str(&out.job).unwrap() == job_num && out.region == *"ap-south-1")
        } else {
            panic!();
        };

        assert!(!aws.instances.contains_key(&job_num.to_string()))
    }

    #[tokio::test(start_paused = true)]
    async fn test_deposit_withdraw_settle() {
        let _ = market::START.set(Instant::now());

        let job_num = H256::from_low_u64_be(1);
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).encode()),
            (40, Action::Deposit, (500).encode()),
            (60, Action::Withdraw, (500).encode()),
            (100, Action::Settle, (2, 6).encode()),
            (505, Action::Close, [].into()),
            ].into_iter().map(|x| (x.0, test::get_log(x.1, Bytes::from(x.2), job_num))).collect();

        let start_time = Instant::now();
        // pending stream appended so job stream never ends
        let job_stream = std::pin::pin!(tokio_stream::iter(job_logs.into_iter())
            .then(|(moment, log)| async move {
                let delay = start_time + Duration::from_secs(moment) - Instant::now();
                sleep(delay).await;
                log
            })
            .chain(tokio_stream::pending()));
        let mut aws: TestAws = Default::default();
        let res = market::job_manager_once(
            job_stream,
            &mut aws,
            job_num,
            vec!["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::new(),
            "xyz".into(),
            "123".into(),
        )
        .await;

        // job manager should have finished successfully
        assert_eq!(res, 0);
        println!("{:?}", aws.outcomes);
        let spin_up_tv_sec: Instant;
        if let TestAwsOutcome::SpinUp(out) = &aws.outcomes[0] {
            spin_up_tv_sec = out.time;
            assert!(
                H256::from_str(&out.job).unwrap() == job_num
                    && out.instance_type == "c6a.xlarge"
                    && out.region == "ap-south-1"
                    && out.req_mem == 4096
                    && out.req_vcpu == 2
                    && out.bandwidth == 76
                    && out.eif_url == "https://example.com/enclave.eif"
                    && out.contract_address == "xyz"
                    && out.chain_id == "123"
            )
        } else {
            panic!();
        };

        if let TestAwsOutcome::SpinDown(out) = &aws.outcomes[1] {
            assert_eq!((out.time - spin_up_tv_sec).as_secs(), 205);
            assert!(H256::from_str(&out.job).unwrap() == job_num && out.region == *"ap-south-1")
        } else {
            panic!();
        };

        assert!(!aws.instances.contains_key(&job_num.to_string()))
    }

    #[tokio::test(start_paused = true)]
    async fn test_revise_rate_cancel() {
        let _ = market::START.set(Instant::now());

        let job_num = H256::from_low_u64_be(1);
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).encode()),
            (50, Action::ReviseRateInitiated, (32000000000000u64,0).encode()),
            (100, Action::ReviseRateFinalized, (32000000000000u64,0).encode()),
            (150, Action::ReviseRateInitiated, (60000000000000u64,0).encode()),
            (200, Action::ReviseRateCancelled, [].into()),
            (505, Action::Close, [].into()),
            ].into_iter().map(|x| (x.0, test::get_log(x.1, Bytes::from(x.2), job_num))).collect();

        let start_time = Instant::now();
        // pending stream appended so job stream never ends
        let job_stream = std::pin::pin!(tokio_stream::iter(job_logs.into_iter())
            .then(|(moment, log)| async move {
                let delay = start_time + Duration::from_secs(moment) - Instant::now();
                sleep(delay).await;
                log
            })
            .chain(tokio_stream::pending()));
        let mut aws: TestAws = Default::default();
        let res = market::job_manager_once(
            job_stream,
            &mut aws,
            job_num,
            vec!["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::new(),
            "xyz".into(),
            "123".into(),
        )
        .await;

        // job manager should have finished successfully
        assert_eq!(res, 0);
        println!("{:?}", aws.outcomes);
        let spin_up_tv_sec: Instant;
        if let TestAwsOutcome::SpinUp(out) = &aws.outcomes[0] {
            spin_up_tv_sec = out.time;
            assert!(
                H256::from_str(&out.job).unwrap() == job_num
                    && out.instance_type == "c6a.xlarge"
                    && out.region == "ap-south-1"
                    && out.req_mem == 4096
                    && out.req_vcpu == 2
                    && out.bandwidth == 76
                    && out.eif_url == "https://example.com/enclave.eif"
                    && out.contract_address == "xyz"
                    && out.chain_id == "123"
            )
        } else {
            panic!();
        };

        if let TestAwsOutcome::SpinDown(out) = &aws.outcomes[1] {
            assert_eq!((out.time - spin_up_tv_sec).as_secs(), 205);
            assert!(H256::from_str(&out.job).unwrap() == job_num && out.region == *"ap-south-1")
        } else {
            panic!();
        };

        assert!(!aws.instances.contains_key(&job_num.to_string()))
    }

    #[tokio::test(start_paused = true)]
    async fn test_unsupported_region() {
        let _ = market::START.set(Instant::now());

        let job_num = H256::from_low_u64_be(1);
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-east-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).encode()),
            (505, Action::Close, [].into()),
            ].into_iter().map(|x| (x.0, test::get_log(x.1, Bytes::from(x.2), job_num))).collect();

        let start_time = Instant::now();
        // pending stream appended so job stream never ends
        let job_stream = std::pin::pin!(tokio_stream::iter(job_logs.into_iter())
            .then(|(moment, log)| async move {
                let delay = start_time + Duration::from_secs(moment) - Instant::now();
                sleep(delay).await;
                log
            })
            .chain(tokio_stream::pending()));
        let mut aws: TestAws = Default::default();
        let res = market::job_manager_once(
            job_stream,
            &mut aws,
            job_num,
            vec!["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::new(),
            "xyz".into(),
            "123".into(),
        )
        .await;

        // job manager should have finished successfully
        assert_eq!(res, -2);
        assert!(aws.outcomes.is_empty());
        assert!(!aws.instances.contains_key(&job_num.to_string()))
    }

    #[tokio::test(start_paused = true)]
    async fn test_region_not_found() {
        let _ = market::START.set(Instant::now());

        let job_num = H256::from_low_u64_be(1);
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).encode()),
            (505, Action::Close, [].into()),
            ].into_iter().map(|x| (x.0, test::get_log(x.1, Bytes::from(x.2), job_num))).collect();

        let start_time = Instant::now();
        // pending stream appended so job stream never ends
        let job_stream = std::pin::pin!(tokio_stream::iter(job_logs.into_iter())
            .then(|(moment, log)| async move {
                let delay = start_time + Duration::from_secs(moment) - Instant::now();
                sleep(delay).await;
                log
            })
            .chain(tokio_stream::pending()));
        let mut aws: TestAws = Default::default();
        let res = market::job_manager_once(
            job_stream,
            &mut aws,
            job_num,
            vec!["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::new(),
            "xyz".into(),
            "123".into(),
        )
        .await;

        // job manager should have finished successfully
        assert_eq!(res, -2);
        assert!(aws.outcomes.is_empty());
        assert!(!aws.instances.contains_key(&job_num.to_string()))
    }

    #[tokio::test(start_paused = true)]
    async fn test_instance_type_not_found() {
        let _ = market::START.set(Instant::now());

        let job_num = H256::from_low_u64_be(1);
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).encode()),
            (505, Action::Close, [].into()),
            ].into_iter().map(|x| (x.0, test::get_log(x.1, Bytes::from(x.2), job_num))).collect();

        let start_time = Instant::now();
        // pending stream appended so job stream never ends
        let job_stream = std::pin::pin!(tokio_stream::iter(job_logs.into_iter())
            .then(|(moment, log)| async move {
                let delay = start_time + Duration::from_secs(moment) - Instant::now();
                sleep(delay).await;
                log
            })
            .chain(tokio_stream::pending()));
        let mut aws: TestAws = Default::default();
        let res = market::job_manager_once(
            job_stream,
            &mut aws,
            job_num,
            vec!["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::new(),
            "xyz".into(),
            "123".into(),
        )
        .await;

        // job manager should have finished successfully
        assert_eq!(res, -2);
        assert!(aws.outcomes.is_empty());
        assert!(!aws.instances.contains_key(&job_num.to_string()))
    }

    #[tokio::test(start_paused = true)]
    async fn test_unsupported_instance() {
        let _ = market::START.set(Instant::now());

        let job_num = H256::from_low_u64_be(1);
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.vsmall\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).encode()),
            (505, Action::Close, [].into()),
            ].into_iter().map(|x| (x.0, test::get_log(x.1, Bytes::from(x.2), job_num))).collect();

        let start_time = Instant::now();
        // pending stream appended so job stream never ends
        let job_stream = std::pin::pin!(tokio_stream::iter(job_logs.into_iter())
            .then(|(moment, log)| async move {
                let delay = start_time + Duration::from_secs(moment) - Instant::now();
                sleep(delay).await;
                log
            })
            .chain(tokio_stream::pending()));
        let mut aws: TestAws = Default::default();
        let res = market::job_manager_once(
            job_stream,
            &mut aws,
            job_num,
            vec!["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::new(),
            "xyz".into(),
            "123".into(),
        )
        .await;

        // job manager should have finished successfully
        assert_eq!(res, -2);
        assert!(aws.outcomes.is_empty());
        assert!(!aws.instances.contains_key(&job_num.to_string()))
    }

    #[tokio::test(start_paused = true)]
    async fn test_eif_url_not_found() {
        let _ = market::START.set(Instant::now());

        let job_num = H256::from_low_u64_be(1);
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"instance\":\"c6a.vsmall\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).encode()),
            (505, Action::Close, [].into()),
            ].into_iter().map(|x| (x.0, test::get_log(x.1, Bytes::from(x.2), job_num))).collect();

        let start_time = Instant::now();
        // pending stream appended so job stream never ends
        let job_stream = std::pin::pin!(tokio_stream::iter(job_logs.into_iter())
            .then(|(moment, log)| async move {
                let delay = start_time + Duration::from_secs(moment) - Instant::now();
                sleep(delay).await;
                log
            })
            .chain(tokio_stream::pending()));
        let mut aws: TestAws = Default::default();
        let res = market::job_manager_once(
            job_stream,
            &mut aws,
            job_num,
            vec!["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::new(),
            "xyz".into(),
            "123".into(),
        )
        .await;

        // job manager should have finished successfully
        assert_eq!(res, -2);
        assert!(aws.outcomes.is_empty());
        assert!(!aws.instances.contains_key(&job_num.to_string()))
    }

    #[tokio::test(start_paused = true)]
    async fn test_min_rate() {
        let _ = market::START.set(Instant::now());

        let job_num = H256::from_low_u64_be(1);
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),29000000000000u64,31000u64,market::now_timestamp().as_secs()).encode()),
            (505, Action::Close, [].into()),
            ].into_iter().map(|x| (x.0, test::get_log(x.1, Bytes::from(x.2), job_num))).collect();

        let start_time = Instant::now();
        // pending stream appended so job stream never ends
        let job_stream = std::pin::pin!(tokio_stream::iter(job_logs.into_iter())
            .then(|(moment, log)| async move {
                let delay = start_time + Duration::from_secs(moment) - Instant::now();
                sleep(delay).await;
                log
            })
            .chain(tokio_stream::pending()));
        let mut aws: TestAws = Default::default();
        let res = market::job_manager_once(
            job_stream,
            &mut aws,
            job_num,
            vec!["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::new(),
            "xyz".into(),
            "123".into(),
        )
        .await;

        // job manager should have finished successfully
        assert_eq!(res, 0);
        assert!(aws.outcomes.is_empty());
        assert!(!aws.instances.contains_key(&job_num.to_string()))
    }

    #[tokio::test(start_paused = true)]
    async fn test_rate_exceed_balance() {
        let _ = market::START.set(Instant::now());

        let job_num = H256::from_low_u64_be(1);
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,0u64,market::now_timestamp().as_secs()).encode()),
            (505, Action::Close, [].into()),
            ].into_iter().map(|x| (x.0, test::get_log(x.1, Bytes::from(x.2), job_num))).collect();

        let start_time = Instant::now();
        // pending stream appended so job stream never ends
        let job_stream = std::pin::pin!(tokio_stream::iter(job_logs.into_iter())
            .then(|(moment, log)| async move {
                let delay = start_time + Duration::from_secs(moment) - Instant::now();
                sleep(delay).await;
                log
            })
            .chain(tokio_stream::pending()));
        let mut aws: TestAws = Default::default();
        let res = market::job_manager_once(
            job_stream,
            &mut aws,
            job_num,
            vec!["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::new(),
            "xyz".into(),
            "123".into(),
        )
        .await;

        // job manager should have finished successfully
        assert_eq!(res, 0);
        assert!(aws.outcomes.is_empty());
        assert!(!aws.instances.contains_key(&job_num.to_string()))
    }

    #[tokio::test(start_paused = true)]
    async fn test_withdrawal_exceed_rate() {
        let _ = market::START.set(Instant::now());

        let job_num = H256::from_low_u64_be(1);
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).encode()),
            (350, Action::Withdraw, (30000u64).encode()),
            (500, Action::Close, [].into()),
        ].into_iter().map(|x| (x.0, test::get_log(x.1, Bytes::from(x.2), job_num))).collect();

        let start_time = Instant::now();
        // pending stream appended so job stream never ends
        let job_stream = std::pin::pin!(tokio_stream::iter(job_logs.into_iter())
            .then(|(moment, log)| async move {
                let delay = start_time + Duration::from_secs(moment) - Instant::now();
                sleep(delay).await;
                log
            })
            .chain(tokio_stream::pending()));
        let mut aws: TestAws = Default::default();
        let res = market::job_manager_once(
            job_stream,
            &mut aws,
            job_num,
            vec!["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::new(),
            "xyz".into(),
            "123".into(),
        )
        .await;

        // job manager should have finished successfully
        assert_eq!(res, 0);
        println!("{:?}", aws.outcomes);
        let spin_up_tv_sec: Instant;
        if let TestAwsOutcome::SpinUp(out) = &aws.outcomes[0] {
            spin_up_tv_sec = out.time;
            assert!(
                H256::from_str(&out.job).unwrap() == job_num
                    && out.instance_type == "c6a.xlarge"
                    && out.region == "ap-south-1"
                    && out.req_mem == 4096
                    && out.req_vcpu == 2
                    && out.bandwidth == 76
                    && out.eif_url == "https://example.com/enclave.eif"
                    && out.contract_address == "xyz"
                    && out.chain_id == "123"
            )
        } else {
            panic!();
        };

        if let TestAwsOutcome::SpinDown(out) = &aws.outcomes[1] {
            assert_eq!((out.time - spin_up_tv_sec).as_secs(), 50);
            assert!(H256::from_str(&out.job).unwrap() == job_num && out.region == *"ap-south-1")
        } else {
            panic!();
        };

        assert!(!aws.instances.contains_key(&job_num.to_string()))
    }

    #[tokio::test(start_paused = true)]
    async fn test_revise_rate_lower_higher() {
        let _ = market::START.set(Instant::now());

        let job_num = H256::from_low_u64_be(1);
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).encode()),
            (350, Action::ReviseRateInitiated, (29000000000000u64,0).encode()),
            (400, Action::ReviseRateFinalized, (29000000000000u64,0).encode()),
            (450, Action::ReviseRateInitiated, (31000000000000u64,0).encode()),
            (500, Action::ReviseRateFinalized, (31000000000000u64,0).encode()),
        ].into_iter().map(|x| (x.0, test::get_log(x.1, Bytes::from(x.2), job_num))).collect();

        let start_time = Instant::now();
        // pending stream appended so job stream never ends
        let job_stream = std::pin::pin!(tokio_stream::iter(job_logs.into_iter())
            .then(|(moment, log)| async move {
                let delay = start_time + Duration::from_secs(moment) - Instant::now();
                sleep(delay).await;
                log
            })
            .chain(tokio_stream::pending()));
        let mut aws: TestAws = Default::default();
        let res = market::job_manager_once(
            job_stream,
            &mut aws,
            job_num,
            vec!["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::new(),
            "xyz".into(),
            "123".into(),
        )
        .await;

        // job manager should have finished successfully
        assert_eq!(res, 0);
        println!("{:?}", aws.outcomes);
        let spin_up_tv_sec: Instant;
        if let TestAwsOutcome::SpinUp(out) = &aws.outcomes[0] {
            spin_up_tv_sec = out.time;
            assert!(
                H256::from_str(&out.job).unwrap() == job_num
                    && out.instance_type == "c6a.xlarge"
                    && out.region == "ap-south-1"
                    && out.req_mem == 4096
                    && out.req_vcpu == 2
                    && out.bandwidth == 76
                    && out.eif_url == "https://example.com/enclave.eif"
                    && out.contract_address == "xyz"
                    && out.chain_id == "123"
            )
        } else {
            panic!();
        };

        if let TestAwsOutcome::SpinDown(out) = &aws.outcomes[1] {
            assert_eq!((out.time - spin_up_tv_sec).as_secs(), 50);
            assert!(H256::from_str(&out.job).unwrap() == job_num && out.region == *"ap-south-1")
        } else {
            panic!();
        };

        assert!(!aws.instances.contains_key(&job_num.to_string()))
    }

    #[tokio::test(start_paused = true)]
    async fn test_address_whitelisted() {
        let _ = market::START.set(Instant::now());

        let job_num = H256::from_low_u64_be(1);
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).encode()),
            (500, Action::Close, [].into()),
        ].into_iter().map(|x| (x.0, test::get_log(x.1, Bytes::from(x.2), job_num))).collect();

        let start_time = Instant::now();
        // pending stream appended so job stream never ends
        let job_stream = std::pin::pin!(tokio_stream::iter(job_logs.into_iter())
            .then(|(moment, log)| async move {
                let delay = start_time + Duration::from_secs(moment) - Instant::now();
                sleep(delay).await;
                log
            })
            .chain(tokio_stream::pending()));
        let mut aws: TestAws = Default::default();
        let res = market::job_manager_once(
            job_stream,
            &mut aws,
            job_num,
            vec!["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::from([
                "0x000000000000000000000000000000000000000000000000f020b3e5fc7a49ec".to_string(),
            ]),
            &Vec::new(),
            "xyz".into(),
            "123".into(),
        )
        .await;

        // job manager should have finished successfully
        assert_eq!(res, 0);
        println!("{:?}", aws.outcomes);
        let spin_up_tv_sec: Instant;
        if let TestAwsOutcome::SpinUp(out) = &aws.outcomes[0] {
            spin_up_tv_sec = out.time;
            assert!(
                H256::from_str(&out.job).unwrap() == job_num
                    && out.instance_type == "c6a.xlarge"
                    && out.region == "ap-south-1"
                    && out.req_mem == 4096
                    && out.req_vcpu == 2
                    && out.bandwidth == 76
                    && out.eif_url == "https://example.com/enclave.eif"
                    && out.contract_address == "xyz"
                    && out.chain_id == "123"
            )
        } else {
            panic!();
        };

        if let TestAwsOutcome::SpinDown(out) = &aws.outcomes[1] {
            assert_eq!((out.time - spin_up_tv_sec).as_secs(), 200);
            assert!(H256::from_str(&out.job).unwrap() == job_num && out.region == *"ap-south-1")
        } else {
            panic!();
        };

        assert!(!aws.instances.contains_key(&job_num.to_string()))
    }

    #[tokio::test(start_paused = true)]
    async fn test_address_not_whitelisted() {
        let _ = market::START.set(Instant::now());

        let job_num = H256::from_low_u64_be(1);
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).encode()),
            (500, Action::Close, [].into()),
        ].into_iter().map(|x| (x.0, test::get_log(x.1, Bytes::from(x.2), job_num))).collect();

        let start_time = Instant::now();
        // pending stream appended so job stream never ends
        let job_stream = std::pin::pin!(tokio_stream::iter(job_logs.into_iter())
            .then(|(moment, log)| async move {
                let delay = start_time + Duration::from_secs(moment) - Instant::now();
                sleep(delay).await;
                log
            })
            .chain(tokio_stream::pending()));
        let mut aws: TestAws = Default::default();
        let res = market::job_manager_once(
            job_stream,
            &mut aws,
            job_num,
            vec!["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::from([
                "0x000000000000000000000000000000000000000000000000f020c4f6gc7a56ce".to_string(),
            ]),
            &Vec::new(),
            "xyz".into(),
            "123".into(),
        )
        .await;

        // job manager should have finished successfully
        assert_eq!(res, 0);
        assert!(aws.outcomes.is_empty());
        assert!(!aws.instances.contains_key(&job_num.to_string()))
    }

    #[tokio::test(start_paused = true)]
    async fn test_address_blacklisted() {
        let _ = market::START.set(Instant::now());

        let job_num = H256::from_low_u64_be(1);
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).encode()),
            (500, Action::Close, [].into()),
        ].into_iter().map(|x| (x.0, test::get_log(x.1, Bytes::from(x.2), job_num))).collect();

        let start_time = Instant::now();
        // pending stream appended so job stream never ends
        let job_stream = std::pin::pin!(tokio_stream::iter(job_logs.into_iter())
            .then(|(moment, log)| async move {
                let delay = start_time + Duration::from_secs(moment) - Instant::now();
                sleep(delay).await;
                log
            })
            .chain(tokio_stream::pending()));
        let mut aws: TestAws = Default::default();
        let res = market::job_manager_once(
            job_stream,
            &mut aws,
            job_num,
            vec!["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::from([
                "0x000000000000000000000000000000000000000000000000f020b3e5fc7a49ec".to_string(),
            ]),
            "xyz".into(),
            "123".into(),
        )
        .await;

        // job manager should have finished successfully
        assert_eq!(res, 0);
        assert!(aws.outcomes.is_empty());
        assert!(!aws.instances.contains_key(&job_num.to_string()))
    }

    #[tokio::test(start_paused = true)]
    async fn test_address_not_blacklisted() {
        let _ = market::START.set(Instant::now());

        let job_num = H256::from_low_u64_be(1);
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).encode()),
            (500, Action::Close, [].into()),
        ].into_iter().map(|x| (x.0, test::get_log(x.1, Bytes::from(x.2), job_num))).collect();

        let start_time = Instant::now();
        // pending stream appended so job stream never ends
        let job_stream = std::pin::pin!(tokio_stream::iter(job_logs.into_iter())
            .then(|(moment, log)| async move {
                let delay = start_time + Duration::from_secs(moment) - Instant::now();
                sleep(delay).await;
                log
            })
            .chain(tokio_stream::pending()));
        let mut aws: TestAws = Default::default();
        let res = market::job_manager_once(
            job_stream,
            &mut aws,
            job_num,
            vec!["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::from([
                "0x000000000000000000000000000000000000000000000000f020b3e5fc7a49ece".to_string(),
            ]),
            "xyz".into(),
            "123".into(),
        )
        .await;

        // job manager should have finished successfully
        assert_eq!(res, 0);
        println!("{:?}", aws.outcomes);
        let spin_up_tv_sec: Instant;
        if let TestAwsOutcome::SpinUp(out) = &aws.outcomes[0] {
            spin_up_tv_sec = out.time;
            assert!(
                H256::from_str(&out.job).unwrap() == job_num
                    && out.instance_type == "c6a.xlarge"
                    && out.region == "ap-south-1"
                    && out.req_mem == 4096
                    && out.req_vcpu == 2
                    && out.bandwidth == 76
                    && out.eif_url == "https://example.com/enclave.eif"
                    && out.contract_address == "xyz"
                    && out.chain_id == "123"
            )
        } else {
            panic!();
        };

        if let TestAwsOutcome::SpinDown(out) = &aws.outcomes[1] {
            assert_eq!((out.time - spin_up_tv_sec).as_secs(), 200);
            assert!(H256::from_str(&out.job).unwrap() == job_num && out.region == *"ap-south-1")
        } else {
            panic!();
        };

        assert!(!aws.instances.contains_key(&job_num.to_string()))
    }

    // Tests for whitelist blacklist checks
    #[tokio::test]
    async fn test_whitelist_blacklist_check_no_list() {
        let _ = market::START.set(Instant::now());

        let log = test::get_log(Action::Open,
            Bytes::from(("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).encode()),
            H256::zero());
        let address_whitelist = vec![];
        let address_blacklist = vec![];

        assert!(market::whitelist_blacklist_check(
            log.clone(),
            &address_whitelist,
            &address_blacklist
        ));
    }

    #[tokio::test]
    async fn test_whitelist_blacklist_check_whitelisted() {
        let _ = market::START.set(Instant::now());

        let log = test::get_log(Action::Open,
            Bytes::from(("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).encode()),
            H256::zero());
        let address_whitelist = vec![
            "0x000000000000000000000000000000000000000000000000f020b3e5fc7a49ec".to_string(),
            "0x000000000000000000000000000000000000000000000000f020b3e5fd6sd76d".to_string(),
        ];
        let address_blacklist = vec![];

        assert!(market::whitelist_blacklist_check(
            log.clone(),
            &address_whitelist,
            &address_blacklist
        ));
    }

    #[tokio::test]
    async fn test_whitelist_blacklist_check_not_whitelisted() {
        let _ = market::START.set(Instant::now());

        let log = test::get_log(Action::Open,
            Bytes::from(("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).encode()),
            H256::zero());
        let address_whitelist = vec![
            "0x000000000000000000000000000000000000000000000000f020b3e5fc7a48as".to_string(),
            "0x000000000000000000000000000000000000000000000000f020b3e5fd6sd76d".to_string(),
        ];
        let address_blacklist = vec![];

        assert!(!market::whitelist_blacklist_check(
            log.clone(),
            &address_whitelist,
            &address_blacklist
        ));
    }

    #[tokio::test]
    async fn test_whitelist_blacklist_check_blacklisted() {
        let _ = market::START.set(Instant::now());

        let log = test::get_log(Action::Open,
            Bytes::from(("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).encode()),
            H256::zero());
        let address_whitelist = vec![];
        let address_blacklist = vec![
            "0x000000000000000000000000000000000000000000000000f020b3e5fc7a49ec".to_string(),
            "0x000000000000000000000000000000000000000000000000f020b3e5fd6sdsd6".to_string(),
        ];

        assert!(!market::whitelist_blacklist_check(
            log.clone(),
            &address_whitelist,
            &address_blacklist
        ));
    }

    #[tokio::test]
    async fn test_whitelist_blacklist_check_not_blacklisted() {
        let _ = market::START.set(Instant::now());

        let log = test::get_log(Action::Open,
            Bytes::from(("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).encode()),
            H256::zero());
        let address_whitelist = vec![];
        let address_blacklist = vec![
            "0x000000000000000000000000000000000000000000000000f020b3e5fc7a49fe".to_string(),
            "0x000000000000000000000000000000000000000000000000f020b3e5fd6sdsd6".to_string(),
        ];

        assert!(market::whitelist_blacklist_check(
            log.clone(),
            &address_whitelist,
            &address_blacklist
        ));
    }

    #[tokio::test]
    async fn test_whitelist_blacklist_check_neither() {
        let _ = market::START.set(Instant::now());

        let log = test::get_log(Action::Open,
            Bytes::from(("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).encode()),
            H256::zero());
        let address_whitelist = vec![
            "0x000000000000000000000000000000000000000000000000f020b3e5fc7a48aa".to_string(),
            "0x000000000000000000000000000000000000000000000000f020b3e5fd6sd76d".to_string(),
        ];
        let address_blacklist = vec![
            "0x000000000000000000000000000000000000000000000000f020b3e5fc7a49ed".to_string(),
            "0x000000000000000000000000000000000000000000000000f020b3e5fd6sdsd6".to_string(),
        ];

        assert!(!market::whitelist_blacklist_check(
            log.clone(),
            &address_whitelist,
            &address_blacklist
        ));
    }

    #[tokio::test]
    async fn test_whitelist_blacklist_check_both() {
        let _ = market::START.set(Instant::now());

        let log = test::get_log(Action::Open,
            Bytes::from(("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).encode()),
            H256::zero());
        let address_whitelist = vec![
            "0x000000000000000000000000000000000000000000000000f020b3e5fc7a49ec".to_string(),
            "0x000000000000000000000000000000000000000000000000f020b3e5fd6sd76d".to_string(),
        ];
        let address_blacklist = vec![
            "0x000000000000000000000000000000000000000000000000f020b3e5fc7a49ec".to_string(),
            "0x000000000000000000000000000000000000000000000000f020b3e5fd6sdsd6".to_string(),
        ];

        assert!(!market::whitelist_blacklist_check(
            log.clone(),
            &address_whitelist,
            &address_blacklist
        ));
    }

    #[test]
    fn test_parse_compute_rates() {
        let contents = "[{\"region\": \"ap-south-1\", \"rate_cards\": [{\"instance\": \"c6a.48xlarge\", \"min_rate\": \"2469600000000000000000\", \"cpu\": 192, \"memory\": 384, \"arch\": \"amd64\"}, {\"instance\": \"m7g.xlarge\", \"min_rate\": \"150000000\", \"cpu\": 4, \"memory\": 8, \"arch\": \"arm64\"}]}]";
        let rates: Vec<market::RegionalRates> = serde_json::from_str(&contents).unwrap();

        assert_eq!(rates.len(), 1);
        assert_eq!(
            rates[0],
            market::RegionalRates {
                region: "ap-south-1".to_owned(),
                rate_cards: vec![
                    market::RateCard {
                        instance: "c6a.48xlarge".to_owned(),
                        min_rate: U256::from_dec_str("2469600000000000000000").unwrap(),
                        cpu: 192,
                        memory: 384,
                        arch: String::from("amd64")
                    },
                    market::RateCard {
                        instance: "m7g.xlarge".to_owned(),
                        min_rate: U256::from(150000000u64),
                        cpu: 4,
                        memory: 8,
                        arch: String::from("arm64")
                    }
                ]
            }
        );
    }

    #[test]
    fn test_parse_bandwidth_rates() {
        let contents = "[{\"region\": \"Asia South (Mumbai)\", \"region_code\": \"ap-south-1\", \"rate\": \"8264900000000000000000\"}, {\"region\": \"US East (N.Virginia)\", \"region_code\": \"us-east-1\", \"rate\": \"10000\"}]";
        let rates: Vec<market::GBRateCard> = serde_json::from_str(&contents).unwrap();

        assert_eq!(rates.len(), 2);
        assert_eq!(
            rates[0],
            market::GBRateCard {
                region: "Asia South (Mumbai)".to_owned(),
                region_code: "ap-south-1".to_owned(),
                rate: U256::from_dec_str("8264900000000000000000").unwrap(),
            }
        );
        assert_eq!(
            rates[1],
            market::GBRateCard {
                region: "US East (N.Virginia)".to_owned(),
                region_code: "us-east-1".to_owned(),
                rate: U256::from(10000u16),
            }
        );
    }
}
