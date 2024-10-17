use std::future::Future;

use alloy::hex::ToHexExt;
use alloy::primitives::{keccak256, Address, B256, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::pubsub::PubSubFrontend;
use alloy::rpc::types::eth::{Filter, Log};
use alloy::sol_types::SolValue;
use alloy::transports::ws::WsConnect;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::time::sleep;
use tokio::time::{Duration, Instant};
use tokio_stream::StreamExt;
use tracing::{error, info, info_span, Instrument};

// IMPORTANT: do not import SystemTime, use the now_timestamp helper

// Basic architecture:
// One future listening to new jobs
// Each job has its own future managing its lifetime

// Identify jobs not only by the id, but also by the operator, contract and the chain
// This is needed to cleanly support multiple operators/contracts/chains at the infra level
#[derive(Clone)]
pub struct JobId {
    pub id: String,
    pub operator: String,
    pub contract: String,
    pub chain: String,
}

pub trait InfraProvider {
    fn spin_up(
        &mut self,
        eif_url: &str,
        job: &JobId,
        instance_type: &str,
        family: &str,
        region: &str,
        req_mem: i64,
        req_vcpu: i32,
        bandwidth: u64,
    ) -> impl Future<Output = Result<String>> + Send;

    fn spin_down(
        &mut self,
        instance_id: &str,
        job: &JobId,
        region: &str,
    ) -> impl Future<Output = Result<()>> + Send;

    fn get_job_instance(
        &self,
        job: &JobId,
        region: &str,
    ) -> impl Future<Output = Result<(bool, String, String)>> + Send;

    fn get_job_ip(&self, job: &JobId, region: &str) -> impl Future<Output = Result<String>> + Send;

    fn check_instance_running(
        &mut self,
        instance_id: &str,
        region: &str,
    ) -> impl Future<Output = Result<bool>> + Send;

    fn check_enclave_running(
        &mut self,
        instance_id: &str,
        region: &str,
    ) -> impl Future<Output = Result<bool>> + Send;

    fn run_enclave(
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
    ) -> impl Future<Output = Result<()>> + Send;

    fn update_enclave_image(
        &mut self,
        instance_id: &str,
        region: &str,
        eif_url: &str,
        req_vcpu: i32,
        req_mem: i64,
    ) -> impl Future<Output = Result<()>> + Send;
}

impl<'a, T> InfraProvider for &'a mut T
where
    T: InfraProvider + Send + Sync,
{
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
        (**self)
            .spin_up(
                eif_url,
                job,
                instance_type,
                family,
                region,
                req_mem,
                req_vcpu,
                bandwidth,
            )
            .await
    }

    async fn spin_down(&mut self, instance_id: &str, job: &JobId, region: &str) -> Result<()> {
        (**self).spin_down(instance_id, job, region).await
    }

    async fn get_job_instance(&self, job: &JobId, region: &str) -> Result<(bool, String, String)> {
        (**self).get_job_instance(job, region).await
    }

    async fn get_job_ip(&self, job: &JobId, region: &str) -> Result<String> {
        (**self).get_job_ip(job, region).await
    }

    async fn check_instance_running(&mut self, instance_id: &str, region: &str) -> Result<bool> {
        (**self).check_instance_running(instance_id, region).await
    }

    async fn check_enclave_running(&mut self, instance_id: &str, region: &str) -> Result<bool> {
        (**self).check_enclave_running(instance_id, region).await
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
        (**self)
            .run_enclave(
                job,
                instance_id,
                family,
                region,
                image_url,
                req_vcpu,
                req_mem,
                bandwidth,
                debug,
            )
            .await
    }

    async fn update_enclave_image(
        &mut self,
        instance_id: &str,
        region: &str,
        eif_url: &str,
        req_vcpu: i32,
        req_mem: i64,
    ) -> Result<()> {
        (**self)
            .update_enclave_image(instance_id, region, eif_url, req_vcpu, req_mem)
            .await
    }
}

pub trait LogsProvider {
    fn new_jobs<'a>(
        &'a self,
        client: &'a impl Provider<PubSubFrontend>,
    ) -> impl Future<Output = Result<impl StreamExt<Item = (B256, bool)> + 'a>>;

    fn job_logs<'a>(
        &'a self,
        client: &'a impl Provider<PubSubFrontend>,
        job: B256,
    ) -> impl Future<Output = Result<impl StreamExt<Item = Log> + Send + 'a>> + Send;
}

#[derive(Clone)]
pub struct EthersProvider {
    pub contract: Address,
    pub provider: Address,
}

impl LogsProvider for EthersProvider {
    async fn new_jobs<'a>(
        &'a self,
        client: &'a impl Provider<PubSubFrontend>,
    ) -> Result<impl StreamExt<Item = (B256, bool)> + 'a> {
        new_jobs(client, self.contract, self.provider).await
    }

    async fn job_logs<'a>(
        &'a self,
        client: &'a impl Provider<PubSubFrontend>,
        job: B256,
    ) -> Result<impl StreamExt<Item = Log> + Send + 'a> {
        job_logs(client, self.contract, job).await
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct RateCard {
    pub instance: String,
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
    pub rate: U256,
}

pub async fn run(
    infra_provider: impl InfraProvider + Send + Sync + Clone + 'static,
    logs_provider: impl LogsProvider + Send + Sync + Clone + 'static,
    url: String,
    regions: &'static [String],
    rates: &'static [RegionalRates],
    gb_rates: &'static [GBRateCard],
    address_whitelist: &'static [String],
    address_blacklist: &'static [String],
    // without job_id.id set
    job_id: JobId,
) {
    let mut backoff = 1;

    // connection level loop
    // start from scratch in case of connection errors
    // trying to implicitly resume connections or event streams can cause issues
    // since subscriptions are stateful

    let mut job_count = 0;
    loop {
        info!("Connecting to RPC endpoint...");
        let res = ProviderBuilder::new()
            .on_ws(WsConnect::new(url.clone()))
            .await;
        if let Err(err) = res {
            // exponential backoff on connection errors
            error!(?err, "Connection error");
            sleep(Duration::from_secs(backoff)).await;
            backoff *= 2;
            if backoff > 128 {
                backoff = 128;
            }
            continue;
        }
        backoff = 1;
        info!("Connected to RPC endpoint");

        let client = res.unwrap();
        let res = logs_provider.new_jobs(&client).await;
        if let Err(err) = res {
            error!(?err, "Subscribe error");
            sleep(Duration::from_secs(1)).await;
            continue;
        }

        let job_stream = std::pin::pin!(res.unwrap());
        job_count += run_once(
            // we need to keep track of jobs for whom tasks have already been spawned
            // and not spawn duplicate tasks
            job_stream.skip(job_count),
            infra_provider.clone(),
            logs_provider.clone(),
            url.clone(),
            regions,
            rates,
            gb_rates,
            address_whitelist,
            address_blacklist,
            job_id.clone(),
        )
        .await;
    }
}

async fn run_once(
    mut job_stream: impl StreamExt<Item = (B256, bool)> + Unpin,
    infra_provider: impl InfraProvider + Send + Sync + Clone + 'static,
    logs_provider: impl LogsProvider + Send + Sync + Clone + 'static,
    url: String,
    regions: &'static [String],
    rates: &'static [RegionalRates],
    gb_rates: &'static [GBRateCard],
    address_whitelist: &'static [String],
    address_blacklist: &'static [String],
    // without job_id.id set
    job_id: JobId,
) -> usize {
    let mut job_count = 0;
    while let Some((job, removed)) = job_stream.next().await {
        info!(?job, removed, "New job");

        // prepare with correct job id
        let mut job_id = job_id.clone();
        job_id.id = job.encode_hex_with_prefix();

        tokio::spawn(
            job_manager(
                infra_provider.clone(),
                logs_provider.clone(),
                url.clone(),
                job_id,
                regions,
                3,
                rates,
                gb_rates,
                address_whitelist,
                address_blacklist,
            )
            .instrument(info_span!(parent: None, "job", ?job)),
        );
        job_count += 1;
    }

    info!("Job stream ended");

    job_count
}

async fn new_jobs(
    client: &impl Provider<PubSubFrontend>,
    address: Address,
    provider: Address,
) -> Result<impl StreamExt<Item = (B256, bool)> + '_> {
    let event_filter = Filter::new()
        .address(address)
        .event_signature(vec![keccak256(
            "JobOpened(bytes32,string,address,address,uint256,uint256,uint256)",
        )])
        .topic3(provider.into_word());

    // ordering is important to prevent race conditions while getting all logs but
    // it still relies on the RPC being consistent between registering the subscription
    // and querying the cutoff block number

    // register subscription
    let stream = client
        .subscribe_logs(&event_filter.clone().select(0..))
        .await
        .context("failed to subscribe to new jobs")?
        .into_stream();

    // get cutoff block number
    let cutoff = client
        .get_block_number()
        .await
        .context("failed to get cutoff block")?;

    // cut off stream at cutoff block, extract data from items
    let stream = stream.filter_map(move |item| {
        if item.block_number.unwrap() > cutoff {
            Some((item.topics()[1], item.removed))
        } else {
            None
        }
    });

    // get logs up to cutoff
    let old_logs = client
        .get_logs(&event_filter.select(0..cutoff))
        .await
        .context("failed to query old logs")?;

    // convert to a stream, extract data from items
    let old_logs = tokio_stream::iter(old_logs).map(|item| (item.topics()[1], item.removed));

    // stream
    let stream = old_logs.chain(stream);

    Ok(stream)
}

// manage the complete lifecycle of a job
async fn job_manager(
    infra_provider: impl InfraProvider + Send + Sync + Clone,
    logs_provider: impl LogsProvider + Send + Sync,
    url: String,
    job_id: JobId,
    allowed_regions: &[String],
    aws_delay_duration: u64,
    rates: &[RegionalRates],
    gb_rates: &[GBRateCard],
    address_whitelist: &[String],
    address_blacklist: &[String],
) {
    let mut backoff = 1;
    let job = job_id.id.clone();

    // connection level loop
    // start from scratch in case of connection errors
    // trying to implicitly resume connections or event streams can cause issues
    // since subscriptions are stateful
    loop {
        info!("Connecting to RPC endpoint...");
        let res = ProviderBuilder::new()
            .on_ws(WsConnect::new(url.clone()))
            .await;
        if let Err(err) = res {
            // exponential backoff on connection errors
            error!(?err, "Connection error");
            sleep(Duration::from_secs(backoff)).await;
            backoff *= 2;
            if backoff > 128 {
                backoff = 128;
            }
            continue;
        }
        backoff = 1;
        info!("Connected to RPC endpoint");

        let client = res.unwrap();
        let res = logs_provider
            // TODO: Bad unwrap?
            .job_logs(&client, job.parse().unwrap())
            .await;
        if let Err(err) = res {
            error!(?err, "Subscribe error");
            sleep(Duration::from_secs(1)).await;
            continue;
        }

        let job_stream = std::pin::pin!(res.unwrap());
        let res = job_manager_once(
            job_stream,
            infra_provider.clone(),
            job_id.clone(),
            allowed_regions,
            aws_delay_duration,
            rates,
            gb_rates,
            address_whitelist,
            address_blacklist,
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
        info!("Checking address whitelist...");
        if address_whitelist
            .iter()
            .any(|s| s == &log.topics()[2].encode_hex_with_prefix())
        {
            info!("ADDRESS ALLOWED!");
        } else {
            info!("ADDRESS NOT ALLOWED!");
            return false;
        }
    }

    // check blacklist
    if !address_blacklist.is_empty() {
        info!("Checking address blacklist...");
        if address_blacklist
            .iter()
            .any(|s| s == &log.topics()[2].encode_hex_with_prefix())
        {
            info!("ADDRESS NOT ALLOWED!");
            return false;
        } else {
            info!("ADDRESS ALLOWED!");
        }
    }

    true
}

struct JobState<'a> {
    job_id: JobId,
    launch_delay: u64,
    allowed_regions: &'a [String],

    balance: U256,
    last_settled: Duration,
    rate: U256,
    original_rate: U256,
    instance_id: String,
    family: String,
    min_rate: U256,
    bandwidth: u64,
    eif_url: String,
    instance_type: String,
    region: String,
    req_vcpus: i32,
    req_mem: i64,
    debug: bool,

    // whether instance should exist or not
    infra_state: bool,
    // how long to wait for infra change
    infra_change_time: Instant,
    // whether to schedule change
    infra_change_scheduled: bool,
    // whether to just update the eif
    eif_update: bool,
}

impl<'a> JobState<'a> {
    fn new(job_id: JobId, launch_delay: u64, allowed_regions: &[String]) -> JobState {
        // solvency metrics
        // default of 60s
        JobState {
            job_id,
            launch_delay,
            allowed_regions,
            balance: U256::from(360),
            last_settled: now_timestamp(),
            rate: U256::from(1),
            original_rate: U256::from(1),
            instance_id: String::new(),
            // salmon is the default for jobs (usually old) without any family specified
            family: "salmon".to_owned(),
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
            eif_update: false,
            debug: false,
        }
    }

    fn insolvency_duration(&self) -> Duration {
        let now_ts = now_timestamp();

        if self.rate == U256::ZERO {
            Duration::from_secs(0)
        } else {
            // solvent for balance / rate seconds from last_settled with 300s as margin
            Duration::from_secs(
                (self.balance * U256::from(10).pow(U256::from(12)) / self.rate)
                    .saturating_to::<u64>()
                    .saturating_sub(300),
            )
            .saturating_sub(now_ts.saturating_sub(self.last_settled))
        }
    }

    async fn heartbeat_check(&mut self, mut infra_provider: impl InfraProvider) {
        let is_running = infra_provider
            .check_instance_running(&self.instance_id, &self.region)
            .await;
        match is_running {
            Err(err) => {
                error!(?err, "Failed to retrieve instance state");
            }
            Ok(is_running) => {
                if is_running {
                    let is_enclave_running = infra_provider
                        .check_enclave_running(&self.instance_id, &self.region)
                        .await;

                    match is_enclave_running {
                        Ok(is_enclave_running) => {
                            if !is_enclave_running {
                                info!("Enclave not running on the instance, running the enclave");
                                let res = infra_provider
                                    .run_enclave(
                                        &self.job_id,
                                        &self.instance_id,
                                        &self.family,
                                        &self.region,
                                        &self.eif_url,
                                        self.req_vcpus,
                                        self.req_mem,
                                        self.bandwidth,
                                        self.debug,
                                    )
                                    .await;
                                match res {
                                    Ok(_) => {
                                        info!("Enclave successfully ran on the instance");
                                    }
                                    Err(err) => {
                                        error!(?err, "Failed to run enclave");
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            error!(?err, "Failed to retrieve enclave state");
                        }
                    }
                } else if !is_running && self.rate >= self.min_rate {
                    info!("Instance not running, scheduling new launch");
                    self.schedule_launch(0);
                }
            }
        }
    }

    fn handle_insolvency(&mut self) {
        info!("INSOLVENCY");
        self.schedule_termination(0);
    }

    fn schedule_launch(&mut self, delay: u64) {
        self.infra_change_scheduled = true;
        self.infra_change_time = Instant::now()
            .checked_add(Duration::from_secs(delay))
            .unwrap();
        self.infra_state = true;
        info!("Instance launch scheduled");
    }

    fn schedule_termination(&mut self, delay: u64) {
        self.infra_change_scheduled = true;
        self.infra_change_time = Instant::now()
            .checked_add(Duration::from_secs(delay))
            .unwrap();
        self.infra_state = false;
        info!("Instance termination scheduled");
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
        let res = infra_provider
            .get_job_instance(&self.job_id, &self.region)
            .await;

        if let Err(err) = res {
            error!(?err, "Failed to get job instance");
            return false;
        }
        let (exist, instance, state) = res.unwrap();

        if self.infra_state {
            // launch mode
            if exist {
                // instance exists already
                if state == "pending" || state == "running" {
                    // instance exists and is already running, we are done
                    info!(instance, "Found existing healthy instance");
                    self.instance_id = instance;
                    if self.eif_update {
                        // update eif
                        let res = infra_provider
                            .update_enclave_image(
                                &self.instance_id,
                                &self.region,
                                &self.eif_url,
                                self.req_vcpus,
                                self.req_mem,
                            )
                            .await;
                        if let Err(err) = res {
                            error!(?err, "Failed to update eif");
                            return false;
                        }
                        self.eif_update = false;
                    }
                    return true;
                }

                if state == "stopping" || state == "stopped" {
                    // instance unhealthy, terminate
                    info!(instance, "Found existing unhealthy instance");
                    let res = infra_provider
                        .spin_down(&instance, &self.job_id, &self.region)
                        .await;
                    if let Err(err) = res {
                        error!(?err, "Failed to terminate instance");
                        return false;
                    }
                }

                // state is shutting-down or terminated at this point
            }

            // either no old instance or old instance was not enough, launch new one
            info!("Launching new instance");
            let res = infra_provider
                .spin_up(
                    self.eif_url.as_str(),
                    &self.job_id,
                    self.instance_type.as_str(),
                    self.family.as_str(),
                    &self.region,
                    self.req_mem,
                    self.req_vcpus,
                    self.bandwidth,
                )
                .await;
            if let Err(err) = res {
                error!(?err, "Instance launch failed");
                return false;
            }
            self.instance_id = res.unwrap();
            info!(self.instance_id, "Instance launched");

            // try to run the enclave, ignore errors
            let res = infra_provider
                .run_enclave(
                    &self.job_id,
                    &self.instance_id,
                    &self.family,
                    &self.region,
                    &self.eif_url,
                    self.req_vcpus,
                    self.req_mem,
                    self.bandwidth,
                    self.debug,
                )
                .await;
            if let Err(err) = res {
                error!(?err, "Enclave launch failed");
                // NOTE: return true here and let heartbeat check pick up from the errors
                return true;
            }
        } else {
            // terminate mode
            if !exist || state == "shutting-down" || state == "terminated" {
                // instance does not really exist anyway, we are done
                info!("Instance does not exist or is already terminated");
                return true;
            }

            // terminate instance
            info!(instance, "Terminating existing instance");
            let res = infra_provider
                .spin_down(&instance, &self.job_id, &self.region)
                .await;
            if let Err(err) = res {
                error!(?err, "Failed to terminate instance");
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
        if log.is_none() {
            // error in the stream, can retry with new conn
            return -1;
        }

        let log = log.unwrap();
        info!(topic = ?log.topics()[0], data = ?log.data(), "New log");

        // events
        #[allow(non_snake_case)]
        let JOB_OPENED =
            keccak256("JobOpened(bytes32,string,address,address,uint256,uint256,uint256)");
        #[allow(non_snake_case)]
        let JOB_SETTLED = keccak256("JobSettled(bytes32,uint256,uint256)");
        #[allow(non_snake_case)]
        let JOB_CLOSED = keccak256("JobClosed(bytes32)");
        #[allow(non_snake_case)]
        let JOB_DEPOSITED = keccak256("JobDeposited(bytes32,address,uint256)");
        #[allow(non_snake_case)]
        let JOB_WITHDREW = keccak256("JobWithdrew(bytes32,address,uint256)");
        #[allow(non_snake_case)]
        let JOB_REVISE_RATE_INITIATED = keccak256("JobReviseRateInitiated(bytes32,uint256)");
        #[allow(non_snake_case)]
        let JOB_REVISE_RATE_CANCELLED = keccak256("JobReviseRateCancelled(bytes32)");
        #[allow(non_snake_case)]
        let JOB_REVISE_RATE_FINALIZED = keccak256("JobReviseRateFinalized(bytes32,uint256)");
        #[allow(non_snake_case)]
        let METADATA_UPDATED = keccak256("JobMetadataUpdated(bytes32,string)");

        // NOTE: jobs should be killed fully if any individual event would kill it
        // regardless of future events
        // helps preserve consistency on restarts where events are procesed all at once
        // e.g. do not spin up if job goes below min_rate and then goes above min_rate

        if log.topics()[0] == JOB_OPENED {
            // decode
            let Ok((metadata, _rate, _balance, timestamp)) =
                <(String, U256, U256, U256)>::abi_decode_sequence(&log.data().data, true)
                    .inspect_err(|err| error!(?err, data = ?log.data(), "OPENED: Decode failure"))
            else {
                return -2;
            };

            info!(
                metadata,
                rate = _rate.to_string(),
                balance = _balance.to_string(),
                timestamp = timestamp.to_string(),
                last_settled = self.last_settled.as_secs(),
                "OPENED",
            );

            // update solvency metrics
            self.balance = _balance;
            self.rate = _rate;
            self.original_rate = _rate;
            self.last_settled = Duration::from_secs(timestamp.saturating_to::<u64>());

            let Ok(v) = serde_json::from_str::<Value>(&metadata)
                .inspect_err(|err| error!(?err, "Error reading metadata"))
            else {
                return -2;
            };

            let Some(t) = v["instance"].as_str() else {
                error!("Instance type not set");
                return -2;
            };
            self.instance_type = t.to_string();
            info!(self.instance_type, "Instance type set");

            let Some(t) = v["region"].as_str() else {
                error!("Job region not set");
                return -2;
            };
            self.region = t.to_string();
            info!(self.region, "Job region set");

            if !self.allowed_regions.contains(&self.region) {
                error!(self.region, "Region not suppported, exiting job");
                return -2;
            }

            let Some(t) = v["memory"].as_i64() else {
                error!("Memory not set");
                return -2;
            };
            self.req_mem = t;
            info!(self.req_mem, "Required memory");

            let Some(t) = v["vcpu"].as_i64() else {
                error!("vcpu not set");
                return -2;
            };
            self.req_vcpus = t.try_into().unwrap_or(i32::MAX);
            info!(self.req_vcpus, "Required vcpu");

            let Some(url) = v["url"].as_str() else {
                error!("EIF url not found! Exiting job");
                return -2;
            };
            self.eif_url = url.to_string();

            // we leave the default family unchanged if not found for backward compatibility
            v["family"]
                .as_str()
                .inspect(|f| self.family = (*f).to_owned());

            // we leave the default debug mode unchanged if not found for backward compatibility
            v["debug"].as_bool().inspect(|f| self.debug = *f);

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
                error!(self.instance_type, "Instance type not supported",);
                return -2;
            }

            info!(
                self.instance_type,
                rate = self.min_rate.to_string(),
                "MIN RATE",
            );

            // launch only if rate is more than min
            if self.rate >= self.min_rate {
                for entry in gb_rates {
                    if entry.region_code == self.region {
                        let gb_cost = entry.rate;
                        let bandwidth_rate = self.rate - self.min_rate;

                        self.bandwidth =
                            (bandwidth_rate.saturating_mul(U256::from(1024 * 1024 * 8)) / gb_cost)
                                .saturating_to::<u64>();
                        break;
                    }
                }
                self.schedule_launch(self.launch_delay);
            } else {
                self.schedule_termination(0);
            }
        } else if log.topics()[0] == JOB_SETTLED {
            // decode
            let Ok((amount, timestamp)) =
                <(U256, U256)>::abi_decode_sequence(&log.data().data, true)
                    .inspect_err(|err| error!(?err, data = ?log.data(), "SETTLED: Decode failure"))
            else {
                return -2;
            };

            info!(
                amount = amount.to_string(),
                rate = self.rate.to_string(),
                balance = self.balance.to_string(),
                last_settled = self.last_settled.as_secs(),
                "SETTLED",
            );
            // update solvency metrics
            self.balance -= amount;
            self.last_settled = Duration::from_secs(timestamp.saturating_to::<u64>());
            info!(
                amount = amount.to_string(),
                rate = self.rate.to_string(),
                balance = self.balance.to_string(),
                last_settled = self.last_settled.as_secs(),
                "SETTLED",
            );
        } else if log.topics()[0] == JOB_CLOSED {
            self.schedule_termination(0);
        } else if log.topics()[0] == JOB_DEPOSITED {
            // decode
            // IMPORTANT: Tuples have to be decoded using abi_decode_sequence
            // if this is changed in the future
            let Ok(amount) = U256::abi_decode(&log.data().data, true)
                .inspect_err(|err| error!(?err, data = ?log.data(), "DEPOSITED: Decode failure"))
            else {
                return -2;
            };

            info!(
                amount = amount.to_string(),
                rate = self.rate.to_string(),
                balance = self.balance.to_string(),
                last_settled = self.last_settled.as_secs(),
                "DEPOSITED",
            );
            // update solvency metrics
            self.balance += amount;
            info!(
                amount = amount.to_string(),
                rate = self.rate.to_string(),
                balance = self.balance.to_string(),
                last_settled = self.last_settled.as_secs(),
                "DEPOSITED",
            );
        } else if log.topics()[0] == JOB_WITHDREW {
            // decode
            // IMPORTANT: Tuples have to be decoded using abi_decode_sequence
            // if this is changed in the future
            let Ok(amount) = U256::abi_decode(&log.data().data, true)
                .inspect_err(|err| error!(?err, data = ?log.data(), "WITHDREW: Decode failure"))
            else {
                return -2;
            };

            info!(
                amount = amount.to_string(),
                rate = self.rate.to_string(),
                balance = self.balance.to_string(),
                last_settled = self.last_settled.as_secs(),
                "WITHDREW",
            );
            // update solvency metrics
            self.balance -= amount;
            info!(
                amount = amount.to_string(),
                rate = self.rate.to_string(),
                balance = self.balance.to_string(),
                last_settled = self.last_settled.as_secs(),
                "WITHDREW",
            );
        } else if log.topics()[0] == JOB_REVISE_RATE_INITIATED {
            // IMPORTANT: Tuples have to be decoded using abi_decode_sequence
            // if this is changed in the future
            let Ok(new_rate) = U256::abi_decode(&log.data().data, true).inspect_err(
                |err| error!(?err, data = ?log.data(), "JOB_REVISE_RATE_INTIATED: Decode failure"),
            ) else {
                return -2;
            };

            info!(
                self.original_rate = self.original_rate.to_string(),
                rate = self.rate.to_string(),
                balance = self.balance.to_string(),
                last_settled = self.last_settled.as_secs(),
                "JOB_REVISE_RATE_INTIATED",
            );
            self.original_rate = self.rate;
            self.rate = new_rate;
            if self.rate < self.min_rate {
                self.schedule_termination(0);
                info!("Revised job rate below min rate, shut down");
            }
            info!(
                self.original_rate = self.original_rate.to_string(),
                rate = self.rate.to_string(),
                balance = self.balance.to_string(),
                last_settled = self.last_settled.as_secs(),
                "JOB_REVISE_RATE_INTIATED",
            );
        } else if log.topics()[0] == JOB_REVISE_RATE_CANCELLED {
            info!(
                rate = self.rate.to_string(),
                balance = self.balance.to_string(),
                last_settled = self.last_settled.as_secs(),
                "JOB_REVISE_RATE_CANCELLED",
            );
            self.rate = self.original_rate;
            info!(
                rate = self.rate.to_string(),
                balance = self.balance.to_string(),
                last_settled = self.last_settled.as_secs(),
                "JOB_REVISE_RATE_CANCELLED",
            );
        } else if log.topics()[0] == JOB_REVISE_RATE_FINALIZED {
            // IMPORTANT: Tuples have to be decoded using abi_decode_sequence
            // if this is changed in the future
            let Ok(new_rate) = U256::abi_decode(&log.data().data, true).inspect_err(
                |err| error!(?err, data = ?log.data(), "JOB_REVISE_RATE_FINALIZED: Decode failure"),
            ) else {
                return -2;
            };

            info!(
                self.original_rate = self.original_rate.to_string(),
                rate = self.rate.to_string(),
                balance = self.balance.to_string(),
                last_settled = self.last_settled.as_secs(),
                "JOB_REVISE_RATE_FINALIZED",
            );
            if self.rate != new_rate {
                error!("Something went wrong, finalized rate not same as initiated rate");
                return -2;
            }
            self.original_rate = new_rate;
            info!(
                self.original_rate = self.original_rate.to_string(),
                rate = self.rate.to_string(),
                balance = self.balance.to_string(),
                last_settled = self.last_settled.as_secs(),
                "JOB_REVISE_RATE_FINALIZED",
            );
        } else if log.topics()[0] == METADATA_UPDATED {
            // IMPORTANT: Tuples have to be decoded using abi_decode_sequence
            // if this is changed in the future
            let Ok(metadata) = String::abi_decode(&log.data().data, true).inspect_err(
                |err| error!(?err, data = ?log.data(), "METADATA_UPDATED: Decode failure"),
            ) else {
                error!(data = ?log.data(), "METADATA_UPDATED: Decode failure");
                return -2;
            };

            info!(metadata, "METADATA_UPDATED");

            let Ok(v) = serde_json::from_str::<Value>(&metadata)
                .inspect_err(|err| error!(?err, "Error reading metadata"))
            else {
                self.schedule_termination(0);
                return -2;
            };

            let Some(t) = v["instance"].as_str() else {
                error!("Instance type not set");
                self.schedule_termination(0);
                return -2;
            };
            if self.instance_type != t {
                error!("Instance type change not allowed");
                self.schedule_termination(0);
                return -2;
            }

            let Some(t) = v["region"].as_str() else {
                error!("Job region not set");
                self.schedule_termination(0);
                return -2;
            };
            if self.region != t {
                error!("Region change not allowed");
                self.schedule_termination(0);
                return -2;
            }

            let Some(t) = v["memory"].as_i64() else {
                error!("Memory not set");
                self.schedule_termination(0);
                return -2;
            };
            if self.req_mem != t {
                error!("Memory change not allowed");
                self.schedule_termination(0);
                return -2;
            }

            let Some(t) = v["vcpu"].as_i64() else {
                error!("vcpu not set");
                self.schedule_termination(0);
                return -2;
            };
            if self.req_vcpus != t.try_into().unwrap_or(2) {
                error!("vcpu change not allowed");
                self.schedule_termination(0);
                return -2;
            }

            let family = v["family"].as_str();
            if family.is_some() && self.family != family.unwrap() {
                error!("Family change not allowed");
                self.schedule_termination(0);
                return -2;
            }

            let debug = v["debug"].as_bool();
            if self.debug != debug.unwrap_or(false) {
                error!("Debug change not allowed");
                self.schedule_termination(0);
                return -2;
            }

            let Some(url) = v["url"].as_str() else {
                error!("EIF url not found! Exiting job");
                self.schedule_termination(0);
                return -2;
            };
            if self.eif_url == url {
                error!("No url change for EIF update event");
                self.schedule_termination(0);
                return -2;
            }
            self.eif_url = url.to_string();
            self.eif_update = true;
            self.schedule_launch(self.launch_delay);
        } else {
            error!(topic = ?log.topics()[0], "Unknown event");
            return -2;
        }

        0
    }
}

// manage the complete lifecycle of a job
// returns true if "done"
async fn job_manager_once(
    mut job_stream: impl StreamExt<Item = Log> + Unpin,
    mut infra_provider: impl InfraProvider + Send + Sync,
    job_id: JobId,
    allowed_regions: &[String],
    aws_delay_duration: u64,
    rates: &[RegionalRates],
    gb_rates: &[GBRateCard],
    address_whitelist: &[String],
    address_blacklist: &[String],
) -> i8 {
    let mut state = JobState::new(job_id, aws_delay_duration, allowed_regions);

    let res = 'event: loop {
        // compute time to insolvency
        let insolvency_duration = state.insolvency_duration();
        info!(duration = insolvency_duration.as_secs(), "Insolvency after");

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

    if res == 0 {
        info!(res, "Job stream ended");
    } else {
        error!(res, "Job stream ended");
    }

    res
}

async fn job_logs(
    client: &impl Provider<PubSubFrontend>,
    contract: Address,
    job: B256,
) -> Result<impl StreamExt<Item = Log> + Send + '_> {
    let event_filter = Filter::new()
        .address(contract)
        .event_signature(vec![
            keccak256("JobOpened(bytes32,string,address,address,uint256,uint256,uint256)"),
            keccak256("JobSettled(bytes32,uint256,uint256)"),
            keccak256("JobClosed(bytes32)"),
            keccak256("JobDeposited(bytes32,address,uint256)"),
            keccak256("JobWithdrew(bytes32,address,uint256)"),
            keccak256("JobReviseRateInitiated(bytes32,uint256)"),
            keccak256("JobReviseRateCancelled(bytes32)"),
            keccak256("JobReviseRateFinalized(bytes32,uint256)"),
            keccak256("JobMetadataUpdated(bytes32,string)"),
        ])
        .topic1(job);

    // ordering is important to prevent race conditions while getting all logs but
    // it still relies on the RPC being consistent between registering the subscription
    // and querying the cutoff block number

    // register subscription
    let stream = client
        .subscribe_logs(&event_filter.clone().select(0..))
        .await
        .context("failed to subscribe to job logs")?
        .into_stream();

    // get cutoff block number
    let cutoff = client
        .get_block_number()
        .await
        .context("failed to get cutoff block")?;

    // cut off stream at cutoff block
    let stream = stream.filter_map(move |item| {
        if item.block_number.unwrap() > cutoff {
            Some(item)
        } else {
            None
        }
    });

    // get logs up to cutoff
    let old_logs = client
        .get_logs(&event_filter.select(0..cutoff))
        .await
        .context("failed to query old logs")?;

    // convert to a stream, extract data from items
    let old_logs = tokio_stream::iter(old_logs);

    // stream
    let stream = old_logs.chain(stream);

    Ok(stream)
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
    use std::str::FromStr;

    use alloy::hex::ToHexExt;
    use alloy::primitives::{Bytes, B256, U256};
    use alloy::rpc::types::eth::Log;
    use alloy::sol_types::SolValue;
    use tokio::time::{sleep, Duration, Instant};
    use tokio_stream::StreamExt;

    use crate::market;
    use crate::test::{self, Action, TestAws, TestAwsOutcome};

    #[tokio::test(start_paused = true)]
    async fn test_instance_launch_after_delay_on_spin_up() {
        let _ = market::START.set(Instant::now());

        let job_num = U256::from(1).into();
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).abi_encode_sequence()),
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
            market::JobId {
                id: job_num.encode_hex_with_prefix(),
                operator: "abc".into(),
                contract: "xyz".into(),
                chain: "123".into(),
            },
            &["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::new(),
        )
        .await;

        // job manager should have finished successfully
        assert_eq!(res, 0);
        let spin_up_tv_sec: Instant;
        let instance_id: String;
        if let TestAwsOutcome::SpinUp(out) = &aws.outcomes[0] {
            spin_up_tv_sec = out.time;
            instance_id = out.instance_id.clone();
            assert!(B256::from_str(&out.job).unwrap() == job_num);
            assert!(out.instance_type == "c6a.xlarge");
            assert!(out.family == "salmon");
            assert!(out.region == "ap-south-1");
            assert!(out.req_mem == 4096);
            assert!(out.req_vcpu == 2);
            assert!(out.bandwidth == 76);
            assert!(out.eif_url == "https://example.com/enclave.eif");
            assert!(out.contract_address == "xyz");
            assert!(out.chain_id == "123");
        } else {
            panic!();
        };

        if let TestAwsOutcome::RunEnclave(out) = &aws.outcomes[1] {
            assert!(B256::from_str(&out.job).unwrap() == job_num);
            assert!(out.instance_id == instance_id);
            assert!(out.family == "salmon");
            assert!(out.region == "ap-south-1");
            assert!(out.req_mem == 4096);
            assert!(out.req_vcpu == 2);
            assert!(out.bandwidth == 76);
            assert!(out.eif_url == "https://example.com/enclave.eif");
            assert!(out.debug == false);
        } else {
            panic!();
        };

        if let TestAwsOutcome::SpinDown(out) = &aws.outcomes[2] {
            assert_eq!((out.time - spin_up_tv_sec).as_secs(), 1);
            assert!(B256::from_str(&out.job).unwrap() == job_num);
            assert!(out.region == *"ap-south-1");
        } else {
            panic!();
        };

        assert!(!aws.instances.contains_key(&job_num.to_string()))
    }

    #[tokio::test(start_paused = true)]
    async fn test_instance_launch_with_debug_mode_on_spin_up() {
        let _ = market::START.set(Instant::now());

        let job_num = U256::from(1).into();
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2,\"debug\":true}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).abi_encode_sequence()),
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
            market::JobId {
                id: job_num.encode_hex_with_prefix(),
                operator: "abc".into(),
                contract: "xyz".into(),
                chain: "123".into(),
            },
            &["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::new(),
        )
        .await;

        // job manager should have finished successfully
        assert_eq!(res, 0);
        let spin_up_tv_sec: Instant;
        let instance_id: String;
        if let TestAwsOutcome::SpinUp(out) = &aws.outcomes[0] {
            spin_up_tv_sec = out.time;
            instance_id = out.instance_id.clone();
            assert!(B256::from_str(&out.job).unwrap() == job_num);
            assert!(out.instance_type == "c6a.xlarge");
            assert!(out.family == "salmon");
            assert!(out.region == "ap-south-1");
            assert!(out.req_mem == 4096);
            assert!(out.req_vcpu == 2);
            assert!(out.bandwidth == 76);
            assert!(out.eif_url == "https://example.com/enclave.eif");
            assert!(out.contract_address == "xyz");
            assert!(out.chain_id == "123");
        } else {
            panic!();
        };

        if let TestAwsOutcome::RunEnclave(out) = &aws.outcomes[1] {
            assert!(B256::from_str(&out.job).unwrap() == job_num);
            assert!(out.instance_id == instance_id);
            assert!(out.family == "salmon");
            assert!(out.region == "ap-south-1");
            assert!(out.req_mem == 4096);
            assert!(out.req_vcpu == 2);
            assert!(out.bandwidth == 76);
            assert!(out.eif_url == "https://example.com/enclave.eif");
            assert!(out.debug == true);
        } else {
            panic!();
        };

        if let TestAwsOutcome::SpinDown(out) = &aws.outcomes[2] {
            assert_eq!((out.time - spin_up_tv_sec).as_secs(), 1);
            assert!(B256::from_str(&out.job).unwrap() == job_num);
            assert!(out.region == *"ap-south-1");
        } else {
            panic!();
        };

        assert!(!aws.instances.contains_key(&job_num.to_string()))
    }

    #[tokio::test(start_paused = true)]
    async fn test_instance_launch_after_delay_on_spin_up_with_specific_family() {
        let _ = market::START.set(Instant::now());

        let job_num = U256::from(1).into();
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2,\"family\":\"tuna\"}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).abi_encode_sequence()),
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
            market::JobId {
                id: job_num.encode_hex_with_prefix(),
                operator: "abc".into(),
                contract: "xyz".into(),
                chain: "123".into(),
            },
            &["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::new(),
        )
        .await;

        // job manager should have finished successfully
        assert_eq!(res, 0);
        let spin_up_tv_sec: Instant;
        let instance_id: String;
        if let TestAwsOutcome::SpinUp(out) = &aws.outcomes[0] {
            spin_up_tv_sec = out.time;
            instance_id = out.instance_id.clone();
            assert!(B256::from_str(&out.job).unwrap() == job_num);
            assert!(out.instance_id == instance_id);
            assert!(out.family == "tuna");
            assert!(out.region == "ap-south-1");
            assert!(out.req_mem == 4096);
            assert!(out.req_vcpu == 2);
            assert!(out.bandwidth == 76);
            assert!(out.eif_url == "https://example.com/enclave.eif");
        } else {
            panic!();
        };

        if let TestAwsOutcome::RunEnclave(out) = &aws.outcomes[1] {
            assert!(B256::from_str(&out.job).unwrap() == job_num);
            assert!(out.instance_id == instance_id);
            assert!(out.family == "tuna");
            assert!(out.region == "ap-south-1");
            assert!(out.req_mem == 4096);
            assert!(out.req_vcpu == 2);
            assert!(out.bandwidth == 76);
            assert!(out.eif_url == "https://example.com/enclave.eif");
            assert!(out.debug == false);
        } else {
            panic!();
        };

        if let TestAwsOutcome::SpinDown(out) = &aws.outcomes[2] {
            assert_eq!((out.time - spin_up_tv_sec).as_secs(), 1);
            assert!(B256::from_str(&out.job).unwrap() == job_num);
            assert!(out.region == *"ap-south-1");
        } else {
            panic!();
        };

        assert!(!aws.instances.contains_key(&job_num.to_string()))
    }

    #[tokio::test(start_paused = true)]
    async fn test_deposit_withdraw_settle() {
        let _ = market::START.set(Instant::now());

        let job_num = U256::from(1).into();
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).abi_encode_sequence()),
            (40, Action::Deposit, 500.abi_encode()),
            (60, Action::Withdraw, 500.abi_encode()),
            (100, Action::Settle, (2, 6).abi_encode_sequence()),
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
            market::JobId {
                id: job_num.encode_hex_with_prefix(),
                operator: "abc".into(),
                contract: "xyz".into(),
                chain: "123".into(),
            },
            &["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::new(),
        )
        .await;

        // job manager should have finished successfully
        assert_eq!(res, 0);
        let spin_up_tv_sec: Instant;
        let instance_id: String;
        if let TestAwsOutcome::SpinUp(out) = &aws.outcomes[0] {
            spin_up_tv_sec = out.time;
            instance_id = out.instance_id.clone();
            assert!(B256::from_str(&out.job).unwrap() == job_num);
            assert!(out.instance_type == "c6a.xlarge");
            assert!(out.family == "salmon");
            assert!(out.region == "ap-south-1");
            assert!(out.req_mem == 4096);
            assert!(out.req_vcpu == 2);
            assert!(out.bandwidth == 76);
            assert!(out.eif_url == "https://example.com/enclave.eif");
            assert!(out.contract_address == "xyz");
            assert!(out.chain_id == "123");
        } else {
            panic!();
        };

        if let TestAwsOutcome::RunEnclave(out) = &aws.outcomes[1] {
            assert!(B256::from_str(&out.job).unwrap() == job_num);
            assert!(out.instance_id == instance_id);
            assert!(out.family == "salmon");
            assert!(out.region == "ap-south-1");
            assert!(out.req_mem == 4096);
            assert!(out.req_vcpu == 2);
            assert!(out.bandwidth == 76);
            assert!(out.eif_url == "https://example.com/enclave.eif");
            assert!(out.debug == false);
        } else {
            panic!();
        };

        if let TestAwsOutcome::SpinDown(out) = &aws.outcomes[2] {
            assert_eq!((out.time - spin_up_tv_sec).as_secs(), 205);
            assert!(B256::from_str(&out.job).unwrap() == job_num);
            assert!(out.region == *"ap-south-1");
        } else {
            panic!();
        };

        assert!(!aws.instances.contains_key(&job_num.to_string()))
    }

    #[tokio::test(start_paused = true)]
    async fn test_revise_rate_cancel() {
        let _ = market::START.set(Instant::now());

        let job_num = U256::from(1).into();
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).abi_encode_sequence()),
            (50, Action::ReviseRateInitiated, 32000000000000u64.abi_encode()),
            (100, Action::ReviseRateFinalized, 32000000000000u64.abi_encode()),
            (150, Action::ReviseRateInitiated, 60000000000000u64.abi_encode()),
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
            market::JobId {
                id: job_num.encode_hex_with_prefix(),
                operator: "abc".into(),
                contract: "xyz".into(),
                chain: "123".into(),
            },
            &["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::new(),
        )
        .await;

        // job manager should have finished successfully
        assert_eq!(res, 0);
        let spin_up_tv_sec: Instant;
        let instance_id: String;
        if let TestAwsOutcome::SpinUp(out) = &aws.outcomes[0] {
            spin_up_tv_sec = out.time;
            instance_id = out.instance_id.clone();
            assert!(B256::from_str(&out.job).unwrap() == job_num);
            assert!(out.instance_type == "c6a.xlarge");
            assert!(out.family == "salmon");
            assert!(out.region == "ap-south-1");
            assert!(out.req_mem == 4096);
            assert!(out.req_vcpu == 2);
            assert!(out.bandwidth == 76);
            assert!(out.eif_url == "https://example.com/enclave.eif");
            assert!(out.contract_address == "xyz");
            assert!(out.chain_id == "123");
        } else {
            panic!();
        };

        if let TestAwsOutcome::RunEnclave(out) = &aws.outcomes[1] {
            assert!(B256::from_str(&out.job).unwrap() == job_num);
            assert!(out.instance_id == instance_id);
            assert!(out.family == "salmon");
            assert!(out.region == "ap-south-1");
            assert!(out.req_mem == 4096);
            assert!(out.req_vcpu == 2);
            assert!(out.bandwidth == 76);
            assert!(out.eif_url == "https://example.com/enclave.eif");
            assert!(out.debug == false);
        } else {
            panic!();
        };

        if let TestAwsOutcome::SpinDown(out) = &aws.outcomes[2] {
            assert_eq!((out.time - spin_up_tv_sec).as_secs(), 205);
            assert!(B256::from_str(&out.job).unwrap() == job_num);
            assert!(out.region == *"ap-south-1");
        } else {
            panic!();
        };

        assert!(!aws.instances.contains_key(&job_num.to_string()))
    }

    #[tokio::test(start_paused = true)]
    async fn test_unsupported_region() {
        let _ = market::START.set(Instant::now());

        let job_num = U256::from(1).into();
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-east-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).abi_encode_sequence()),
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
            market::JobId {
                id: job_num.encode_hex_with_prefix(),
                operator: "abc".into(),
                contract: "xyz".into(),
                chain: "123".into(),
            },
            &["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::new(),
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

        let job_num = U256::from(1).into();
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).abi_encode_sequence()),
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
            market::JobId {
                id: job_num.encode_hex_with_prefix(),
                operator: "abc".into(),
                contract: "xyz".into(),
                chain: "123".into(),
            },
            &["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::new(),
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

        let job_num = U256::from(1).into();
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).abi_encode_sequence()),
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
            market::JobId {
                id: job_num.encode_hex_with_prefix(),
                operator: "abc".into(),
                contract: "xyz".into(),
                chain: "123".into(),
            },
            &["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::new(),
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

        let job_num = U256::from(1).into();
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.vsmall\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).abi_encode_sequence()),
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
            market::JobId {
                id: job_num.encode_hex_with_prefix(),
                operator: "abc".into(),
                contract: "xyz".into(),
                chain: "123".into(),
            },
            &["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::new(),
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

        let job_num = U256::from(1).into();
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"instance\":\"c6a.vsmall\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).abi_encode_sequence()),
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
            market::JobId {
                id: job_num.encode_hex_with_prefix(),
                operator: "abc".into(),
                contract: "xyz".into(),
                chain: "123".into(),
            },
            &["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::new(),
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

        let job_num = U256::from(1).into();
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),29000000000000u64,31000u64,market::now_timestamp().as_secs()).abi_encode_sequence()),
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
            market::JobId {
                id: job_num.encode_hex_with_prefix(),
                operator: "abc".into(),
                contract: "xyz".into(),
                chain: "123".into(),
            },
            &["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::new(),
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

        let job_num = U256::from(1).into();
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,0u64,market::now_timestamp().as_secs()).abi_encode_sequence()),
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
            market::JobId {
                id: job_num.encode_hex_with_prefix(),
                operator: "abc".into(),
                contract: "xyz".into(),
                chain: "123".into(),
            },
            &["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::new(),
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

        let job_num = U256::from(1).into();
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).abi_encode_sequence()),
            (350, Action::Withdraw, 30000u64.abi_encode()),
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
            market::JobId {
                id: job_num.encode_hex_with_prefix(),
                operator: "abc".into(),
                contract: "xyz".into(),
                chain: "123".into(),
            },
            &["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::new(),
        )
        .await;

        // job manager should have finished successfully
        assert_eq!(res, 0);
        let spin_up_tv_sec: Instant;
        let instance_id: String;
        if let TestAwsOutcome::SpinUp(out) = &aws.outcomes[0] {
            spin_up_tv_sec = out.time;
            instance_id = out.instance_id.clone();
            assert!(
                B256::from_str(&out.job).unwrap() == job_num
                    && out.instance_type == "c6a.xlarge"
                    && out.family == "salmon"
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

        if let TestAwsOutcome::RunEnclave(out) = &aws.outcomes[1] {
            assert!(
                B256::from_str(&out.job).unwrap() == job_num
                    && out.instance_id == instance_id
                    && out.family == "salmon"
                    && out.region == "ap-south-1"
                    && out.req_mem == 4096
                    && out.req_vcpu == 2
                    && out.bandwidth == 76
                    && out.eif_url == "https://example.com/enclave.eif"
                    && out.debug == false
            )
        } else {
            panic!();
        };

        if let TestAwsOutcome::SpinDown(out) = &aws.outcomes[2] {
            assert_eq!((out.time - spin_up_tv_sec).as_secs(), 50);
            assert!(B256::from_str(&out.job).unwrap() == job_num && out.region == *"ap-south-1")
        } else {
            panic!();
        };

        assert!(!aws.instances.contains_key(&job_num.to_string()))
    }

    #[tokio::test(start_paused = true)]
    async fn test_revise_rate_lower_higher() {
        let _ = market::START.set(Instant::now());

        let job_num = U256::from(1).into();
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).abi_encode_sequence()),
            (350, Action::ReviseRateInitiated, 29000000000000u64.abi_encode()),
            (400, Action::ReviseRateFinalized, 29000000000000u64.abi_encode()),
            (450, Action::ReviseRateInitiated, 31000000000000u64.abi_encode()),
            (500, Action::ReviseRateFinalized, 31000000000000u64.abi_encode()),
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
            market::JobId {
                id: job_num.encode_hex_with_prefix(),
                operator: "abc".into(),
                contract: "xyz".into(),
                chain: "123".into(),
            },
            &["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::new(),
        )
        .await;

        // job manager should have finished successfully
        assert_eq!(res, 0);
        let spin_up_tv_sec: Instant;
        let instance_id: String;
        if let TestAwsOutcome::SpinUp(out) = &aws.outcomes[0] {
            spin_up_tv_sec = out.time;
            instance_id = out.instance_id.clone();
            assert!(B256::from_str(&out.job).unwrap() == job_num);
            assert!(out.instance_type == "c6a.xlarge");
            assert!(out.family == "salmon");
            assert!(out.region == "ap-south-1");
            assert!(out.req_mem == 4096);
            assert!(out.req_vcpu == 2);
            assert!(out.bandwidth == 76);
            assert!(out.eif_url == "https://example.com/enclave.eif");
            assert!(out.contract_address == "xyz");
            assert!(out.chain_id == "123");
        } else {
            panic!();
        };

        if let TestAwsOutcome::RunEnclave(out) = &aws.outcomes[1] {
            assert!(B256::from_str(&out.job).unwrap() == job_num);
            assert!(out.instance_id == instance_id);
            assert!(out.family == "salmon");
            assert!(out.region == "ap-south-1");
            assert!(out.req_mem == 4096);
            assert!(out.req_vcpu == 2);
            assert!(out.bandwidth == 76);
            assert!(out.eif_url == "https://example.com/enclave.eif");
            assert!(out.debug == false);
        } else {
            panic!();
        };

        if let TestAwsOutcome::SpinDown(out) = &aws.outcomes[2] {
            assert_eq!((out.time - spin_up_tv_sec).as_secs(), 50);
            assert!(B256::from_str(&out.job).unwrap() == job_num);
            assert!(out.region == *"ap-south-1");
        } else {
            panic!();
        };

        assert!(!aws.instances.contains_key(&job_num.to_string()))
    }

    #[tokio::test(start_paused = true)]
    async fn test_address_whitelisted() {
        let _ = market::START.set(Instant::now());

        let job_num = U256::from(1).into();
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).abi_encode_sequence()),
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
            market::JobId {
                id: job_num.encode_hex_with_prefix(),
                operator: "abc".into(),
                contract: "xyz".into(),
                chain: "123".into(),
            },
            &["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::from([
                "0x0000000000000000000000000f5f91ba30a00bd43bd19466f020b3e5fc7a49ec".to_string(),
            ]),
            &Vec::new(),
        )
        .await;

        // job manager should have finished successfully
        assert_eq!(res, 0);
        let spin_up_tv_sec: Instant;
        let instance_id: String;
        if let TestAwsOutcome::SpinUp(out) = &aws.outcomes[0] {
            spin_up_tv_sec = out.time;
            instance_id = out.instance_id.clone();
            assert!(B256::from_str(&out.job).unwrap() == job_num);
            assert!(out.instance_type == "c6a.xlarge");
            assert!(out.family == "salmon");
            assert!(out.region == "ap-south-1");
            assert!(out.req_mem == 4096);
            assert!(out.req_vcpu == 2);
            assert!(out.bandwidth == 76);
            assert!(out.eif_url == "https://example.com/enclave.eif");
            assert!(out.contract_address == "xyz");
            assert!(out.chain_id == "123");
        } else {
            panic!();
        };

        if let TestAwsOutcome::RunEnclave(out) = &aws.outcomes[1] {
            assert!(B256::from_str(&out.job).unwrap() == job_num);
            assert!(out.instance_id == instance_id);
            assert!(out.family == "salmon");
            assert!(out.region == "ap-south-1");
            assert!(out.req_mem == 4096);
            assert!(out.req_vcpu == 2);
            assert!(out.bandwidth == 76);
            assert!(out.eif_url == "https://example.com/enclave.eif");
            assert!(out.debug == false);
        } else {
            panic!();
        };

        if let TestAwsOutcome::SpinDown(out) = &aws.outcomes[2] {
            assert_eq!((out.time - spin_up_tv_sec).as_secs(), 200);
            assert!(B256::from_str(&out.job).unwrap() == job_num);
            assert!(out.region == *"ap-south-1");
        } else {
            panic!();
        };

        assert!(!aws.instances.contains_key(&job_num.to_string()))
    }

    #[tokio::test(start_paused = true)]
    async fn test_address_not_whitelisted() {
        let _ = market::START.set(Instant::now());

        let job_num = U256::from(1).into();
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).abi_encode_sequence()),
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
            market::JobId {
                id: job_num.encode_hex_with_prefix(),
                operator: "abc".into(),
                contract: "xyz".into(),
                chain: "123".into(),
            },
            &["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::from([
                "0x0000000000000000000000000f5f91ba30a00bd43bd19466f020b3e5fc7a49ed".to_string(),
            ]),
            &Vec::new(),
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

        let job_num = U256::from(1).into();
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).abi_encode_sequence()),
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
            market::JobId {
                id: job_num.encode_hex_with_prefix(),
                operator: "abc".into(),
                contract: "xyz".into(),
                chain: "123".into(),
            },
            &["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::from([
                "0x0000000000000000000000000f5f91ba30a00bd43bd19466f020b3e5fc7a49ec".to_string(),
            ]),
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

        let job_num = U256::from(1).into();
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).abi_encode_sequence()),
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
            market::JobId {
                id: job_num.encode_hex_with_prefix(),
                operator: "abc".into(),
                contract: "xyz".into(),
                chain: "123".into(),
            },
            &["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::from([
                "0x0000000000000000000000000f5f91ba30a00bd43bd19466f020b3e5fc7a49ed".to_string(),
            ]),
        )
        .await;

        // job manager should have finished successfully
        assert_eq!(res, 0);
        let spin_up_tv_sec: Instant;
        let instance_id: String;
        if let TestAwsOutcome::SpinUp(out) = &aws.outcomes[0] {
            spin_up_tv_sec = out.time;
            instance_id = out.instance_id.clone();
            assert!(B256::from_str(&out.job).unwrap() == job_num);
            assert!(out.instance_type == "c6a.xlarge");
            assert!(out.family == "salmon");
            assert!(out.region == "ap-south-1");
            assert!(out.req_mem == 4096);
            assert!(out.req_vcpu == 2);
            assert!(out.bandwidth == 76);
            assert!(out.eif_url == "https://example.com/enclave.eif");
            assert!(out.contract_address == "xyz");
            assert!(out.chain_id == "123");
        } else {
            panic!();
        };

        if let TestAwsOutcome::RunEnclave(out) = &aws.outcomes[1] {
            assert!(B256::from_str(&out.job).unwrap() == job_num);
            assert!(out.instance_id == instance_id);
            assert!(out.family == "salmon");
            assert!(out.region == "ap-south-1");
            assert!(out.req_mem == 4096);
            assert!(out.req_vcpu == 2);
            assert!(out.bandwidth == 76);
            assert!(out.eif_url == "https://example.com/enclave.eif");
            assert!(out.debug == false);
        } else {
            panic!();
        };

        if let TestAwsOutcome::SpinDown(out) = &aws.outcomes[2] {
            assert_eq!((out.time - spin_up_tv_sec).as_secs(), 200);
            assert!(B256::from_str(&out.job).unwrap() == job_num);
            assert!(out.region == *"ap-south-1");
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
            Bytes::from(("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).abi_encode_sequence()),
            B256::ZERO);
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
            Bytes::from(("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).abi_encode_sequence()),
            B256::ZERO);
        let address_whitelist = vec![
            "0x0000000000000000000000000f5f91ba30a00bd43bd19466f020b3e5fc7a49ec".to_string(),
            "0x0000000000000000000000000f5f91ba30a00bd43bd19466f020b3e5fc7a49ed".to_string(),
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
            Bytes::from(("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).abi_encode_sequence()),
            B256::ZERO);
        let address_whitelist = vec![
            "0x0000000000000000000000000f5f91ba30a00bd43bd19466f020b3e5fc7a49eb".to_string(),
            "0x0000000000000000000000000f5f91ba30a00bd43bd19466f020b3e5fc7a49ed".to_string(),
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
            Bytes::from(("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).abi_encode_sequence()),
            B256::ZERO);
        let address_whitelist = vec![];
        let address_blacklist = vec![
            "0x0000000000000000000000000f5f91ba30a00bd43bd19466f020b3e5fc7a49ec".to_string(),
            "0x0000000000000000000000000f5f91ba30a00bd43bd19466f020b3e5fc7a49ed".to_string(),
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
            Bytes::from(("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).abi_encode_sequence()),
            B256::ZERO);
        let address_whitelist = vec![];
        let address_blacklist = vec![
            "0x0000000000000000000000000f5f91ba30a00bd43bd19466f020b3e5fc7a49eb".to_string(),
            "0x0000000000000000000000000f5f91ba30a00bd43bd19466f020b3e5fc7a49ed".to_string(),
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
            Bytes::from(("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).abi_encode_sequence()),
            B256::ZERO);
        let address_whitelist = vec![
            "0x0000000000000000000000000f5f91ba30a00bd43bd19466f020b3e5fc7a49eb".to_string(),
            "0x0000000000000000000000000f5f91ba30a00bd43bd19466f020b3e5fc7a49ed".to_string(),
        ];
        let address_blacklist = vec![
            "0x0000000000000000000000000f5f91ba30a00bd43bd19466f020b3e5fc7a49ea".to_string(),
            "0x0000000000000000000000000f5f91ba30a00bd43bd19466f020b3e5fc7a49ee".to_string(),
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
            Bytes::from(("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).abi_encode_sequence()),
            B256::ZERO);
        let address_whitelist = vec![
            "0x0000000000000000000000000f5f91ba30a00bd43bd19466f020b3e5fc7a49ec".to_string(),
            "0x0000000000000000000000000f5f91ba30a00bd43bd19466f020b3e5fc7a49ed".to_string(),
        ];
        let address_blacklist = vec![
            "0x0000000000000000000000000f5f91ba30a00bd43bd19466f020b3e5fc7a49ec".to_string(),
            "0x0000000000000000000000000f5f91ba30a00bd43bd19466f020b3e5fc7a49eb".to_string(),
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
        let rates: Vec<market::RegionalRates> = serde_json::from_str(contents).unwrap();

        assert_eq!(rates.len(), 1);
        assert_eq!(
            rates[0],
            market::RegionalRates {
                region: "ap-south-1".to_owned(),
                rate_cards: vec![
                    market::RateCard {
                        instance: "c6a.48xlarge".to_owned(),
                        min_rate: U256::from_str_radix("2469600000000000000000", 10).unwrap(),
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
        let rates: Vec<market::GBRateCard> = serde_json::from_str(contents).unwrap();

        assert_eq!(rates.len(), 2);
        assert_eq!(
            rates[0],
            market::GBRateCard {
                region: "Asia South (Mumbai)".to_owned(),
                region_code: "ap-south-1".to_owned(),
                rate: U256::from_str_radix("8264900000000000000000", 10).unwrap(),
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

    #[tokio::test(start_paused = true)]
    async fn test_eif_update_after_spin_up() {
        let _ = market::START.set(Instant::now());

        let job_num = U256::from(1).into();
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).abi_encode_sequence()),
            (100, Action::MetadataUpdated, "{\"region\":\"ap-south-1\",\"url\":\"https://example.com/updated-enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string().abi_encode()),
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
            market::JobId {
                id: job_num.encode_hex_with_prefix(),
                operator: "abc".into(),
                contract: "xyz".into(),
                chain: "123".into(),
            },
            &["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::new(),
        )
        .await;

        // job manager should have finished successfully
        assert_eq!(res, 0);
        let spin_up_tv_sec: Instant;
        let instance_id: String;
        if let TestAwsOutcome::SpinUp(out) = &aws.outcomes[0] {
            spin_up_tv_sec = out.time;
            instance_id = out.instance_id.clone();
            assert!(B256::from_str(&out.job).unwrap() == job_num);
            assert!(out.instance_type == "c6a.xlarge");
            assert!(out.family == "salmon");
            assert!(out.region == "ap-south-1");
            assert!(out.req_mem == 4096);
            assert!(out.req_vcpu == 2);
            assert!(out.bandwidth == 76);
            assert!(out.eif_url == "https://example.com/updated-enclave.eif");
            assert!(out.contract_address == "xyz");
            assert!(out.chain_id == "123");
        } else {
            panic!();
        };

        if let TestAwsOutcome::RunEnclave(out) = &aws.outcomes[1] {
            assert!(B256::from_str(&out.job).unwrap() == job_num);
            assert!(out.instance_id == instance_id);
            assert!(out.family == "salmon");
            assert!(out.region == "ap-south-1");
            assert!(out.req_mem == 4096);
            assert!(out.req_vcpu == 2);
            assert!(out.bandwidth == 76);
            assert!(out.eif_url == "https://example.com/updated-enclave.eif");
            assert!(out.debug == false);
        } else {
            panic!();
        };

        if let TestAwsOutcome::SpinDown(out) = &aws.outcomes[2] {
            assert_eq!((out.time - spin_up_tv_sec).as_secs(), 105);
            assert!(B256::from_str(&out.job).unwrap() == job_num);
            assert!(out.region == *"ap-south-1");
        } else {
            panic!();
        };

        assert!(!aws.instances.contains_key(&job_num.to_string()))
    }

    #[tokio::test(start_paused = true)]
    async fn test_other_metadata_update_after_spin_up() {
        let _ = market::START.set(Instant::now());

        let job_num = U256::from(1).into();
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).abi_encode_sequence()),
            // instance type has also been updated in the metadata. should fail this job.
            (100, Action::MetadataUpdated, "{\"region\":\"ap-south-1\",\"url\":\"https://example.com/updated-enclave.eif\",\"instance\":\"c6a.large\",\"memory\":4096,\"vcpu\":2}".to_string().abi_encode()),
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
            market::JobId {
                id: job_num.encode_hex_with_prefix(),
                operator: "abc".into(),
                contract: "xyz".into(),
                chain: "123".into(),
            },
            &["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::new(),
        )
        .await;

        // job manager should have errored out
        assert_eq!(res, -2);
        assert!(aws.outcomes.is_empty());
        assert!(!aws.instances.contains_key(&job_num.to_string()))
    }

    #[tokio::test(start_paused = true)]
    async fn test_metadata_update_event_with_no_updates_after_spin_up() {
        let _ = market::START.set(Instant::now());

        let job_num = U256::from(1).into();
        let job_logs: Vec<(u64, Log)> = vec![
            (0, Action::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string(),31000000000000u64,31000u64,market::now_timestamp().as_secs()).abi_encode_sequence()),
            // instance type has also been updated in the metadata. should fail this job.
            (100, Action::MetadataUpdated, "{\"region\":\"ap-south-1\",\"url\":\"https://example.com/enclave.eif\",\"instance\":\"c6a.xlarge\",\"memory\":4096,\"vcpu\":2}".to_string().abi_encode()),
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
            market::JobId {
                id: job_num.encode_hex_with_prefix(),
                operator: "abc".into(),
                contract: "xyz".into(),
                chain: "123".into(),
            },
            &["ap-south-1".into()],
            300,
            &test::get_rates(),
            &test::get_gb_rates(),
            &Vec::new(),
            &Vec::new(),
        )
        .await;

        // job manager should have errored out
        assert_eq!(res, -2);
        assert!(aws.outcomes.is_empty());
        assert!(!aws.instances.contains_key(&job_num.to_string()))
    }
}
