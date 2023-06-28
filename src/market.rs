use async_trait::async_trait;
use ethers::abi::{AbiDecode, AbiEncode};
use ethers::prelude::*;
use ethers::utils::keccak256;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::error::Error;
use std::fs;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::time::sleep;
use tokio::time::{Duration, Instant};
use tokio_stream::Stream;

use ethers::types::Log;
use tokio_stream::StreamExt;

use crate::server;
use crate::test;

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
    ) -> Result<String, Box<dyn Error + Send + Sync>>;

    async fn spin_down(
        &mut self,
        instance_id: &str,
        job: String,
        region: String,
    ) -> Result<bool, Box<dyn Error + Send + Sync>>;

    async fn get_job_instance(
        &mut self,
        job: &str,
        region: String,
    ) -> Result<(bool, String, String), Box<dyn Error + Send + Sync>>;

    async fn check_instance_running(
        &mut self,
        instance_id: &str,
        region: String,
    ) -> Result<bool, Box<dyn Error + Send + Sync>>;
}

#[async_trait]
impl<'a, T> InfraProvider for &'a mut T
where
    T: InfraProvider + Send,
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
    ) -> Result<String, Box<dyn Error + Send + Sync>> {
        (**self)
            .spin_up(
                eif_url,
                job,
                instance_type,
                region,
                req_mem,
                req_vcpu,
                bandwidth,
            )
            .await
    }

    async fn spin_down(
        &mut self,
        instance_id: &str,
        job: String,
        region: String,
    ) -> Result<bool, Box<dyn Error + Send + Sync>> {
        (**self).spin_down(instance_id, job, region).await
    }

    async fn get_job_instance(
        &mut self,
        job: &str,
        region: String,
    ) -> Result<(bool, String, String), Box<dyn Error + Send + Sync>> {
        (**self).get_job_instance(job, region).await
    }

    async fn check_instance_running(
        &mut self,
        instance_id: &str,
        region: String,
    ) -> Result<bool, Box<dyn Error + Send + Sync>> {
        (**self).check_instance_running(instance_id, region).await
    }
}

#[async_trait]
pub trait LogsProvider {
    async fn new_jobs<'a>(
        &'a self,
        client: &'a Provider<Ws>,
    ) -> Result<Box<dyn Stream<Item = (H256, bool)> + 'a>, Box<dyn Error + Send + Sync>>;

    async fn job_logs<'a>(
        &'a self,
        client: &'a Provider<Ws>,
        job: H256,
    ) -> Result<Box<dyn Stream<Item = Log> + Send + 'a>, Box<dyn Error + Send + Sync>>;
}

#[derive(Clone)]
pub struct EthersProvider {
    pub address: String,
    pub provider: String,
}

#[async_trait]
impl LogsProvider for EthersProvider {
    async fn new_jobs<'a>(
        &'a self,
        client: &'a Provider<Ws>,
    ) -> Result<Box<dyn Stream<Item = (H256, bool)> + 'a>, Box<dyn Error + Send + Sync>> {
        new_jobs(client, self.address.clone(), self.provider.clone()).await
    }

    async fn job_logs<'a>(
        &'a self,
        client: &'a Provider<Ws>,
        job: H256,
    ) -> Result<Box<dyn Stream<Item = Log> + Send + 'a>, Box<dyn Error + Send + Sync>> {
        job_logs(client, job).await
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GBRateCard {
    pub region: String,
    pub region_code: String,
    pub rate: i64,
}

pub async fn run(
    infra_provider: impl InfraProvider + Send + Sync + Clone + 'static,
    logs_provider: impl LogsProvider + Send + Sync + Clone + 'static,
    url: String,
    regions: Vec<String>,
    rates_path: String,
    gb_rates_path: String,
    address_whitelist: &Arc<Vec<String>>,
    address_blacklist: &Arc<Vec<String>>,
) {
    let mut backoff = 1;

    // connection level loop
    // start from scratch in case of connection errors
    // trying to implicitly resume connections or event streams can cause issues
    // since subscriptions are stateful

    let file_path = rates_path;
    let contents = fs::read_to_string(file_path);

    if let Err(err) = contents {
        println!("Error reading rates file : {err}");
        return;
    }
    let contents = contents.unwrap();
    let rates: Vec<server::RegionalRates> = serde_json::from_str(&contents).unwrap_or_default();

    let file_path = gb_rates_path;
    let contents = fs::read_to_string(file_path);

    if let Err(err) = contents {
        println!("Error reading bandwidth rates file : {err}");
        return;
    }
    let contents = contents.unwrap();
    let gb_rates: Vec<GBRateCard> = serde_json::from_str(&contents).unwrap_or_default();

    loop {
        println!("main: Connecting to RPC endpoint...");
        let res = Provider::<Ws>::connect(url.clone()).await;
        if let Err(err) = res {
            // exponential backoff on connection errors
            println!("main: Connection error: {err}");
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
            println!("main: Subscribe error: {err}");
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
            &rates,
            &gb_rates,
            address_whitelist,
            address_blacklist,
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
    rates: &Vec<server::RegionalRates>,
    gb_rates: &Vec<GBRateCard>,
    address_whitelist: &Arc<Vec<String>>,
    address_blacklist: &Arc<Vec<String>>,
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
            rates.clone(),
            gb_rates.clone(),
            address_whitelist.clone(),
            address_blacklist.clone(),
        ));
    }

    println!("main: Job stream ended");
}

async fn new_jobs(
    client: &Provider<Ws>,
    address: String,
    provider: String,
) -> Result<Box<dyn Stream<Item = (H256, bool)> + '_>, Box<dyn Error + Send + Sync>> {
    // TODO: Filter by contract and provider address
    let event_filter = Filter::new()
        .address(address.parse::<Address>()?)
        .select(0..)
        .topic0(vec![keccak256(
            "JobOpened(bytes32,string,address,address,uint256,uint256,uint256)",
        )])
        .topic3(provider.parse::<Address>()?);

    // register subscription
    let stream = client.subscribe_logs(&event_filter).await?;

    Ok(Box::new(stream.map(|item| {
        (item.topics[1], item.removed.unwrap_or(false))
    })))
}

// manage the complete lifecycle of a job
async fn job_manager(
    infra_provider: impl InfraProvider + Send + Sync + Clone,
    logs_provider: impl LogsProvider + Send + Sync + Send + Clone,
    url: String,
    job: H256,
    allowed_regions: Vec<String>,
    aws_delay_duration: u64,
    rates: Vec<server::RegionalRates>,
    gb_rates: Vec<GBRateCard>,
    address_whitelist: Arc<Vec<String>>,
    address_blacklist: Arc<Vec<String>>,
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
            println!("job {job}: Connection error: {err}");
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
            println!("job {job}: Subscribe error: {err}");
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
            &rates,
            &gb_rates,
            &address_whitelist,
            &address_blacklist,
        )
        .await;

        if res {
            // full exit
            break;
        }
    }
}

struct JobState {
    job: H256,
    launch_delay: u64,
    allowed_regions: Vec<String>,

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
    fn new(job: H256, launch_delay: u64, allowed_regions: Vec<String>) -> JobState {
        // solvency metrics
        // default of 60s
        JobState {
            job,
            launch_delay,
            allowed_regions,
            balance: U256::from(360),
            last_settled: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap(),
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
        let now_ts = SystemTime::UNIX_EPOCH.elapsed().unwrap();
        let sat_convert = |n: U256| n.clamp(U256::zero(), u64::MAX.into()).low_u64();

        if self.rate == U256::zero() {
            Duration::from_secs(0)
        } else {
            // solvent for balance / rate seconds from last_settled with 300s as margin
            Duration::from_secs(sat_convert(self.balance / self.rate).saturating_sub(300))
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
                println!("job {job}: failed to retrieve instance state, {err}");
            }
            Ok(is_running) => {
                if !is_running && self.rate >= self.min_rate {
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

        let (exist, instance, state) = infra_provider
            .get_job_instance(&job.encode_hex(), self.region.clone())
            .await
            .unwrap_or((false, "".to_string(), "".to_string()));

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
                        println!("job {job}: ERROR failed to terminate instance, {err}");
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
                )
                .await;
            if let Err(err) = res {
                println!("job {job}: Instance launch failed, {err}");
                return false;
            }
            self.instance_id = res.unwrap();
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
                println!("job {job}: ERROR failed to terminate instance, {err}");
                return false;
            }
        }

        true
    }

    fn whitelist_blacklist_check(
        &self,
        log: Log,
        address_whitelist: &Arc<Vec<String>>,
        address_blacklist: &Arc<Vec<String>>,
    ) -> bool {
        // check whitelist
        if !address_whitelist.is_empty() {
            println!("Checking address whitelist...");
            if address_whitelist.iter().any(|s| s == &log.topics[2].encode_hex()) {
                println!("ADDRESS ALLOWED!");
            } else {
                println!("ADDRESS NOT ALLOWED!");
                return false;
            }
        }

        // check blacklist
        if !address_blacklist.is_empty() {
            println!("Checking address blacklist...");
            if address_blacklist.iter().any(|s| s == &log.topics[2].encode_hex()) {
                println!("ADDRESS NOT ALLOWED!");
                return false;
            } else {
                println!("ADDRESS ALLOWED!");
            }
        }

        return true;
    }

    // return 0 on success
    // -1 on recoverable errors (can retry)
    // -2 on unrecoverable errors (no point retrying)
    // -3 when blacklist/whitelist check fails
    fn process_log(
        &mut self,
        log: Option<Log>,
        rates: &Vec<server::RegionalRates>,
        gb_rates: &Vec<GBRateCard>,
        address_whitelist: &Arc<Vec<String>>,
        address_blacklist: &Arc<Vec<String>>,
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
                    "job {job}: OPENED: metadata: {metadata}, rate: {_rate}, balance: {_balance}, timestamp: {}",
                    self.last_settled.as_secs()
                );

                // update solvency metrics
                self.balance = _balance;
                self.rate = _rate;
                self.original_rate = _rate;
                self.last_settled = Duration::from_secs(timestamp.low_u64());
                let v = serde_json::from_str(&metadata);
                if let Err(err) = v {
                    println!("job {job}: Error reading metadata: {err}");
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
                        "job {job}: region : {} not suppported, exiting job",
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
                    self.whitelist_blacklist_check(log.clone(), address_whitelist, address_blacklist);
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
                                self.min_rate = U256::from(card.min_rate);
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

                for entry in gb_rates {
                    if entry.region_code == self.region {
                        let gb_cost = entry.rate;
                        let bandwidth_rate = self.rate - self.min_rate;

                        self.bandwidth = (bandwidth_rate.as_u64() / gb_cost as u64) * 1024 * 1024;
                        break;
                    }
                }

                println!(
                    "job {job}: MIN RATE for {} instance is {}",
                    self.instance_type, self.min_rate
                );

                // launch only if rate is more than min
                if self.rate >= self.min_rate {
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
    mut infra_provider: impl InfraProvider + Send + Sync + Clone,
    job: H256,
    allowed_regions: Vec<String>,
    aws_delay_duration: u64,
    rates: &Vec<server::RegionalRates>,
    gb_rates: &Vec<GBRateCard>,
    address_whitelist: &Arc<Vec<String>>,
    address_blacklist: &Arc<Vec<String>>,
) -> bool {
    let mut state = JobState::new(job, aws_delay_duration, allowed_regions);

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
                if res == -2 {
                    break 'event true;
                } else if res == -1 {
                    break 'event false;
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
                    break 'event res;
                }
            }
        }
    };

    println!("job {job}: Job stream ended: {res}");

    res
}

async fn job_logs(
    client: &Provider<Ws>,
    job: H256,
) -> Result<Box<dyn Stream<Item = Log> + Send + '_>, Box<dyn Error + Send + Sync>> {
    // TODO: Filter by contract and job
    let event_filter = Filter::new()
        .select(0..)
        .address("0x9d95D61eA056721E358BC49fE995caBF3B86A34B".parse::<Address>()?)
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
    let stream = client.subscribe_logs(&event_filter).await?;

    Ok(Box::new(stream))
}

#[derive(Clone)]
pub struct TestLogger {}

#[async_trait]
impl LogsProvider for TestLogger {
    async fn new_jobs<'a>(
        &'a self,
        _client: &'a Provider<Ws>,
    ) -> Result<Box<dyn Stream<Item = (H256, bool)> + 'a>, Box<dyn Error + Send + Sync>> {
        let logs: Vec<Log> = test::test_logs();
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
    ) -> Result<Box<dyn Stream<Item = Log> + Send + 'a>, Box<dyn Error + Send + Sync>> {
        let logs: Vec<Log> = test::test_logs()
            .into_iter()
            .filter(|log| log.topics[1] == job)
            .collect();
        Ok(Box::new(
            tokio_stream::iter(logs).throttle(Duration::from_secs(2)),
        ))
    }
}

#[derive(Clone)]
pub struct TestAws {
    outcomes: Vec<char>,
    cur_idx: i32,
    max_idx: i32,
    outfile: String,
}

use std::fs::OpenOptions;
use std::io::Write;

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
    ) -> Result<String, Box<dyn Error + Send + Sync>> {
        if self.outfile.as_str() != "" {
            let mut file = OpenOptions::new()
                .append(true)
                .open(&self.outfile)
                .expect("Unable to open out file");
            file.write_all("SpinUp\n".as_bytes()).expect("write failed");
        }
        println!(
            "TEST: spin_up | job: {job}, region: {region}, instance_type: {instance_type}, eif_url: {eif_url}, mem: {req_mem}, vcpu: {req_vcpu}, bandwidth: {bandwidth}"
        );
        if self.cur_idx >= self.max_idx || self.outcomes[self.cur_idx as usize] != 'U' {
            println!("TEST FAIL!\nTEST FAIL!\nTEST FAIL!\n");
            return Err("fail".into());
        }
        self.cur_idx += 1;
        Ok("12345".to_string())
    }

    async fn spin_down(
        &mut self,
        instance_id: &str,
        job: String,
        region: String,
    ) -> Result<bool, Box<dyn Error + Send + Sync>> {
        if self.outfile.as_str() != "" {
            let mut file = OpenOptions::new()
                .append(true)
                .open(&self.outfile)
                .expect("Unable to open out file");
            file.write_all("SpinDown\n".as_bytes())
                .expect("write failed");
        }
        println!("TEST: spin_down | job: {job}, instance_id: {instance_id} region: {region}");
        if self.cur_idx >= self.max_idx || self.outcomes[self.cur_idx as usize] != 'D' {
            println!("TEST FAIL!\nTEST FAIL!\nTEST FAIL!\n");
            return Err("fail".into());
        }
        self.cur_idx += 1;
        Ok(true)
    }

    async fn get_job_instance(
        &mut self,
        job: &str,
        region: String,
    ) -> Result<(bool, String, String), Box<dyn Error + Send + Sync>> {
        println!("TEST: get_job_instance | job: {job}, region: {region}");
        Ok((false, "".to_string(), "".to_owned()))
    }

    async fn check_instance_running(
        &mut self,
        _instance_id: &str,
        _region: String,
    ) -> Result<bool, Box<dyn Error + Send + Sync>> {
        // println!("TEST: check_instance_running | instance_id: {}, region: {}", instance_id, region);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use crate::market;
    use crate::server;
    use ethers::prelude::*;
    use std::fs;
    use std::sync::Arc;

    use super::GBRateCard;

    fn get_rates() -> Option<Vec<server::RegionalRates>> {
        let file_path = "./rates.json";
        let contents = fs::read_to_string(file_path);

        if let Err(err) = contents {
            println!("Error reading rates file : {err}");
            return None;
        }
        let contents = contents.unwrap();
        let rates: Vec<server::RegionalRates> = serde_json::from_str(&contents).unwrap_or_default();
        Some(rates)
    }

    fn get_gb_rates() -> Option<Vec<GBRateCard>> {
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

    #[tokio::test]
    async fn test_1() {
        market::job_manager(
            market::TestAws {
                outcomes: vec!['U', 'D'],
                cur_idx: 0,
                max_idx: 2,
                outfile: "".into(),
            },
            market::TestLogger {},
            "wss://arb-goerli.g.alchemy.com/v2/KYCa2H4IoaidJPaStdaPuUlICHYhCWo3".to_string(),
            H256::from_low_u64_be(1),
            vec!["ap-south-1".into()],
            1,
            get_rates().unwrap_or_default(),
            get_gb_rates().unwrap_or_default(),
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_2() {
        market::job_manager(
            market::TestAws {
                outcomes: vec!['U', 'D'],
                cur_idx: 0,
                max_idx: 2,
                outfile: "".into(),
            },
            market::TestLogger {},
            "wss://arb-goerli.g.alchemy.com/v2/KYCa2H4IoaidJPaStdaPuUlICHYhCWo3".to_string(),
            H256::from_low_u64_be(2),
            vec!["ap-south-1".into()],
            1,
            get_rates().unwrap_or_default(),
            get_gb_rates().unwrap_or_default(),
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_3() {
        market::job_manager(
            market::TestAws {
                outcomes: vec!['U', 'D'],
                cur_idx: 0,
                max_idx: 2,
                outfile: "".into(),
            },
            market::TestLogger {},
            "wss://arb-goerli.g.alchemy.com/v2/KYCa2H4IoaidJPaStdaPuUlICHYhCWo3".to_string(),
            H256::from_low_u64_be(3),
            vec!["ap-south-1".into()],
            1,
            get_rates().unwrap_or_default(),
            get_gb_rates().unwrap_or_default(),
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_4() {
        market::job_manager(
            market::TestAws {
                outcomes: vec!['U', 'D'],
                cur_idx: 0,
                max_idx: 2,
                outfile: "".into(),
            },
            market::TestLogger {},
            "wss://arb-goerli.g.alchemy.com/v2/KYCa2H4IoaidJPaStdaPuUlICHYhCWo3".to_string(),
            H256::from_low_u64_be(4),
            vec!["ap-south-1".into()],
            1,
            get_rates().unwrap_or_default(),
            get_gb_rates().unwrap_or_default(),
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_5() {
        market::job_manager(
            market::TestAws {
                outcomes: vec!['U', 'U', 'D'],
                cur_idx: 0,
                max_idx: 3,
                outfile: "".into(),
            },
            market::TestLogger {},
            "wss://arb-goerli.g.alchemy.com/v2/KYCa2H4IoaidJPaStdaPuUlICHYhCWo3".to_string(),
            H256::from_low_u64_be(5),
            vec!["ap-south-1".into()],
            1,
            get_rates().unwrap_or_default(),
            get_gb_rates().unwrap_or_default(),
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_6() {
        market::job_manager(
            market::TestAws {
                outcomes: vec!['U', 'D'],
                cur_idx: 0,
                max_idx: 2,
                outfile: "".into(),
            },
            market::TestLogger {},
            "wss://arb-goerli.g.alchemy.com/v2/KYCa2H4IoaidJPaStdaPuUlICHYhCWo3".to_string(),
            H256::from_low_u64_be(6),
            vec!["ap-south-1".into()],
            1,
            get_rates().unwrap_or_default(),
            get_gb_rates().unwrap_or_default(),
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_7() {
        market::job_manager(
            market::TestAws {
                outcomes: vec![],
                cur_idx: 0,
                max_idx: 0,
                outfile: "".into(),
            },
            market::TestLogger {},
            "wss://arb-goerli.g.alchemy.com/v2/KYCa2H4IoaidJPaStdaPuUlICHYhCWo3".to_string(),
            H256::from_low_u64_be(7),
            vec!["ap-south-1".into()],
            1,
            get_rates().unwrap_or_default(),
            get_gb_rates().unwrap_or_default(),
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_8() {
        market::job_manager(
            market::TestAws {
                outcomes: vec!['U', 'D'],
                cur_idx: 0,
                max_idx: 2,
                outfile: "".into(),
            },
            market::TestLogger {},
            "wss://arb-goerli.g.alchemy.com/v2/KYCa2H4IoaidJPaStdaPuUlICHYhCWo3".to_string(),
            H256::from_low_u64_be(8),
            vec!["ap-south-1".into()],
            1,
            get_rates().unwrap_or_default(),
            get_gb_rates().unwrap_or_default(),
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_9() {
        market::job_manager(
            market::TestAws {
                outcomes: vec!['U', 'D'],
                cur_idx: 0,
                max_idx: 2,
                outfile: "".into(),
            },
            market::TestLogger {},
            "wss://arb-goerli.g.alchemy.com/v2/KYCa2H4IoaidJPaStdaPuUlICHYhCWo3".to_string(),
            H256::from_low_u64_be(9),
            vec!["ap-south-1".into()],
            1,
            get_rates().unwrap_or_default(),
            get_gb_rates().unwrap_or_default(),
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_10() {
        market::job_manager(
            market::TestAws {
                outcomes: vec![],
                cur_idx: 0,
                max_idx: 0,
                outfile: "".into(),
            },
            market::TestLogger {},
            "wss://arb-goerli.g.alchemy.com/v2/KYCa2H4IoaidJPaStdaPuUlICHYhCWo3".to_string(),
            H256::from_low_u64_be(10),
            vec!["ap-south-1".into()],
            1,
            get_rates().unwrap_or_default(),
            get_gb_rates().unwrap_or_default(),
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_11() {
        market::job_manager(
            market::TestAws {
                outcomes: vec![],
                cur_idx: 0,
                max_idx: 0,
                outfile: "".into(),
            },
            market::TestLogger {},
            "wss://arb-goerli.g.alchemy.com/v2/KYCa2H4IoaidJPaStdaPuUlICHYhCWo3".to_string(),
            H256::from_low_u64_be(11),
            vec!["ap-south-1".into()],
            1,
            get_rates().unwrap_or_default(),
            get_gb_rates().unwrap_or_default(),
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_12() {
        market::job_manager(
            market::TestAws {
                outcomes: vec![],
                cur_idx: 0,
                max_idx: 0,
                outfile: "".into(),
            },
            market::TestLogger {},
            "wss://arb-goerli.g.alchemy.com/v2/KYCa2H4IoaidJPaStdaPuUlICHYhCWo3".to_string(),
            H256::from_low_u64_be(12),
            vec!["ap-south-1".into()],
            1,
            get_rates().unwrap_or_default(),
            get_gb_rates().unwrap_or_default(),
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_13() {
        market::job_manager(
            market::TestAws {
                outcomes: vec![],
                cur_idx: 0,
                max_idx: 0,
                outfile: "".into(),
            },
            market::TestLogger {},
            "wss://arb-goerli.g.alchemy.com/v2/KYCa2H4IoaidJPaStdaPuUlICHYhCWo3".to_string(),
            H256::from_low_u64_be(13),
            vec!["ap-south-1".into()],
            1,
            get_rates().unwrap_or_default(),
            get_gb_rates().unwrap_or_default(),
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_14() {
        market::job_manager(
            market::TestAws {
                outcomes: vec!['U', 'D'],
                cur_idx: 0,
                max_idx: 2,
                outfile: "".into(),
            },
            market::TestLogger {},
            "wss://arb-goerli.g.alchemy.com/v2/KYCa2H4IoaidJPaStdaPuUlICHYhCWo3".to_string(),
            H256::from_low_u64_be(14),
            vec!["ap-south-1".into()],
            1,
            get_rates().unwrap_or_default(),
            get_gb_rates().unwrap_or_default(),
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_15() {
        market::job_manager(
            market::TestAws {
                outcomes: vec!['U', 'D', 'U', 'D'],
                cur_idx: 0,
                max_idx: 4,
                outfile: "".into(),
            },
            market::TestLogger {},
            "wss://arb-goerli.g.alchemy.com/v2/KYCa2H4IoaidJPaStdaPuUlICHYhCWo3".to_string(),
            H256::from_low_u64_be(15),
            vec!["ap-south-1".into()],
            1,
            get_rates().unwrap_or_default(),
            get_gb_rates().unwrap_or_default(),
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_16() {
        market::job_manager(
            market::TestAws {
                outcomes: vec!['U', 'D'],
                cur_idx: 0,
                max_idx: 2,
                outfile: "".into(),
            },
            market::TestLogger {},
            "wss://arb-goerli.g.alchemy.com/v2/KYCa2H4IoaidJPaStdaPuUlICHYhCWo3".to_string(),
            H256::from_low_u64_be(1),
            vec!["ap-south-1".into()],
            1,
            get_rates().unwrap_or_default(),
            get_gb_rates().unwrap_or_default(),
            Arc::new(Vec::from(["0x000000000000000000000000000000000000000000000000f020b3e5fc7a49ec".to_string()])),
            Arc::new(Vec::new()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_17() {
        market::job_manager(
            market::TestAws {
                outcomes: vec!['U', 'D'],
                cur_idx: 0,
                max_idx: 2,
                outfile: "".into(),
            },
            market::TestLogger {},
            "wss://arb-goerli.g.alchemy.com/v2/KYCa2H4IoaidJPaStdaPuUlICHYhCWo3".to_string(),
            H256::from_low_u64_be(1),
            vec!["ap-south-1".into()],
            1,
            get_rates().unwrap_or_default(),
            get_gb_rates().unwrap_or_default(),
            Arc::new(Vec::from(["0x000000000000000000000000000000000000000000000000f020c4f6gc7a56ce".to_string()])),
            Arc::new(Vec::new()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_18() {
        market::job_manager(
            market::TestAws {
                outcomes: vec!['U', 'D'],
                cur_idx: 0,
                max_idx: 2,
                outfile: "".into(),
            },
            market::TestLogger {},
            "wss://arb-goerli.g.alchemy.com/v2/KYCa2H4IoaidJPaStdaPuUlICHYhCWo3".to_string(),
            H256::from_low_u64_be(1),
            vec!["ap-south-1".into()],
            1,
            get_rates().unwrap_or_default(),
            get_gb_rates().unwrap_or_default(),
            Arc::new(Vec::new()),
            Arc::new(Vec::from(["0x000000000000000000000000000000000000000000000000f020b3e5fc7a49ec".to_string()])),
        )
        .await;
    }

    #[tokio::test]
    async fn test_19() {
        market::job_manager(
            market::TestAws {
                outcomes: vec!['U', 'D'],
                cur_idx: 0,
                max_idx: 2,
                outfile: "".into(),
            },
            market::TestLogger {},
            "wss://arb-goerli.g.alchemy.com/v2/KYCa2H4IoaidJPaStdaPuUlICHYhCWo3".to_string(),
            H256::from_low_u64_be(1),
            vec!["ap-south-1".into()],
            1,
            get_rates().unwrap_or_default(),
            get_gb_rates().unwrap_or_default(),
            Arc::new(Vec::new()),
            Arc::new(Vec::from(["0x000000000000000000000000000000000000000000000000f020c4f6gc7a56ce".to_string()])),
        )
        .await;
    }
}
