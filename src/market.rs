use async_trait::async_trait;
use ethers::abi::AbiDecode;
use ethers::prelude::*;
use ethers::utils::keccak256;
use serde_json::Value;
use std::error::Error;
use std::fs;
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

pub struct JobsService {}

#[async_trait]
pub trait AwsManager {
    async fn spin_up(
        &mut self,
        eif_url: &str,
        job: String,
        instance_type: &str,
        region: String,
        req_mem: i64,
        req_vcpu: i32,
    ) -> Result<String, Box<dyn Error + Send + Sync>>;

    async fn spin_down(
        &mut self,
        instance_id: &str,
        region: String,
    ) -> Result<bool, Box<dyn Error + Send + Sync>>;

    async fn get_job_instance(
        &mut self,
        job: &str,
        region: String,
    ) -> Result<(bool, String), Box<dyn Error + Send + Sync>>;

    async fn check_instance_running(
        &mut self,
        instance_id: &str,
        region: String,
    ) -> Result<bool, Box<dyn Error + Send + Sync>>;
}

#[async_trait]
pub trait Logger {
    async fn new_jobs<'a>(
        &'a self,
        client: &'a Provider<Ws>,
    ) -> Result<Box<dyn Stream<Item = (H256, bool)> + 'a>, Box<dyn Error + Send + Sync + 'a>>;

    async fn job_logs<'a>(
        &'a self,
        client: &'a Provider<Ws>,
        job: H256,
    ) -> Result<Box<dyn Stream<Item = Log> + Send + 'a>, Box<dyn Error + Send + Sync + 'a>>;
}

#[derive(Clone)]
pub struct RealLogger {}

#[async_trait]
impl Logger for RealLogger {
    async fn new_jobs<'a>(
        &'a self,
        client: &'a Provider<Ws>,
    ) -> Result<Box<dyn Stream<Item = (H256, bool)> + 'a>, Box<dyn Error + Send + Sync + 'a>> {
        JobsService::new_jobs(client).await
    }

    async fn job_logs<'a>(
        &'a self,
        client: &'a Provider<Ws>,
        job: H256,
    ) -> Result<Box<dyn Stream<Item = Log> + Send + 'a>, Box<dyn Error + Send + Sync + 'a>> {
        JobsService::job_logs(client, job).await
    }
}

impl JobsService {
    pub async fn run(
        aws_manager_impl: impl AwsManager + Send + Sync + Clone + 'static,
        logger_impl: impl Logger + Send + Sync + Clone + 'static,
        url: String,
        regions: Vec<String>,
        rates_path: String,
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
            let res = logger_impl.new_jobs(&client).await;
            if let Err(err) = res {
                println!("main: Subscribe error: {err}");
                sleep(Duration::from_secs(1)).await;
                continue;
            }

            let mut job_stream = Box::into_pin(res.unwrap());
            while let Some((job, removed)) = job_stream.next().await {
                println!("main: New job: {job}, {removed}");
                tokio::spawn(Self::job_manager(
                    aws_manager_impl.clone(),
                    logger_impl.clone(),
                    url.clone(),
                    job,
                    regions.clone(),
                    3,
                    rates_path.clone(),
                ));
            }

            println!("main: Job stream ended");
        }
    }

    async fn new_jobs(
        client: &Provider<Ws>,
    ) -> Result<Box<dyn Stream<Item = (H256, bool)> + '_>, Box<dyn Error + Send + Sync + '_>> {
        // TODO: Filter by contract and provider address
        let event_filter = Filter::new()
            .address("0x9d95D61eA056721E358BC49fE995caBF3B86A34B".parse::<Address>()?)
            .select(0..)
            .topic0(vec![keccak256(
                "JobOpened(bytes32,string,address,address,uint256,uint256,uint256)",
            )]);

        // register subscription
        let stream = client.subscribe_logs(&event_filter).await?;

        Ok(Box::new(stream.map(|item| {
            (item.topics[1], item.removed.unwrap_or(false))
        })))
    }

    // manage the complete lifecycle of a job
    async fn job_manager(
        mut aws_manager_impl: impl AwsManager + Send + Sync + Clone,
        logger_impl: impl Logger + Send + Sync + Send,
        url: String,
        job: H256,
        allowed_regions: Vec<String>,
        aws_delay_duration: u64,
        rates_path: String,
    ) {
        let mut backoff = 1;

        // connection level loop
        // start from scratch in case of connection errors
        // trying to implicitly resume connections or event streams can cause issues
        // since subscriptions are stateful
        'main: loop {
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
            let res = logger_impl.job_logs(&client, job).await;
            if let Err(err) = res {
                println!("job {job}: Subscribe error: {err}");
                sleep(Duration::from_secs(1)).await;
                continue;
            }

            // events
            #[allow(non_snake_case)]
            let JOB_OPENED =
                keccak256("JobOpened(bytes32,string,address,address,uint256,uint256,uint256)")
                    .into();
            #[allow(non_snake_case)]
            let JOB_SETTLED = keccak256("JobSettled(bytes32,uint256,uint256)").into();
            #[allow(non_snake_case)]
            let JOB_CLOSED = keccak256("JobClosed(bytes32)").into();
            #[allow(non_snake_case)]
            let JOB_DEPOSITED = keccak256("JobDeposited(bytes32,address,uint256)").into();
            #[allow(non_snake_case)]
            let JOB_WITHDREW = keccak256("JobWithdrew(bytes32,address,uint256)").into();
            #[allow(non_snake_case)]
            let JOB_REVISE_RATE_INITIATED =
                keccak256("JobReviseRateInitiated(bytes32,uint256)").into();
            #[allow(non_snake_case)]
            let JOB_REVISE_RATE_CANCELLED = keccak256("JobReviseRateCancelled(bytes32)").into();
            #[allow(non_snake_case)]
            let JOB_REVISE_RATE_FINALIZED =
                keccak256("JobReviseRateFinalized(bytes32, uint256)").into();

            // solvency metrics
            // default of 60s
            let mut balance = U256::from(60);
            let mut last_settled = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap();
            let mut rate = U256::one();
            let mut original_rate = U256::one();
            let mut instance_id = String::new();
            let mut job_stream = Box::into_pin(res.unwrap());
            let mut min_rate = U256::one();
            let mut eif_url = String::new();
            let mut instance_type = "c6a.xlarge".to_string();
            let mut region = "ap-south-1".to_string();
            let mut aws_launch_time = Instant::now();
            let mut aws_launch_scheduled = false;
            let mut req_vcpus: i32 = 2;
            let mut req_mem: i64 = 4096;
            'event: loop {
                // compute time to insolvency
                let now_ts = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap();
                fn sat_convert(n: U256) -> u64 {
                    let lowu64 = n.low_u64();
                    if n == lowu64.into() {
                        lowu64
                    } else {
                        u64::MAX
                    }
                }

                // NOTE: should add margin for node to spin down?
                let insolvency_duration = if rate == U256::zero() {
                    Duration::from_secs(0)
                } else {
                    Duration::from_secs(sat_convert(balance / rate))
                        .saturating_sub(now_ts.saturating_sub(last_settled))
                };
                println!(
                    "job {}: Insolvency after: {}",
                    job,
                    insolvency_duration.as_secs()
                );

                let aws_delay_timeout = if aws_launch_scheduled {
                    aws_launch_time.saturating_duration_since(Instant::now())
                } else {
                    insolvency_duration + Duration::from_secs(100)
                };

                tokio::select! {
                    // running instance heartbeat check
                    () = sleep(Duration::from_secs(5)) => {
                        if instance_id.as_str() != "" {
                            let running = aws_manager_impl.check_instance_running(&instance_id, region.clone()).await;
                            if let Err(err) = running {
                                println!("job {job}: failed to retrieve instance state, {err}");
                                if rate >= min_rate {
                                    let res = aws_manager_impl.spin_up(eif_url.as_str(), job.to_string(), instance_type.as_str(), region.clone(), req_mem, req_vcpus).await;
                                    if let Err(err) = res {
                                        println!("job {job}: Instance launch failed, {err}");
                                        break 'event;
                                    }
                                    instance_id = res.unwrap();
                                }
                            } else {
                                let running = running.unwrap();
                                if !running && rate >= min_rate {
                                    let res = aws_manager_impl.spin_up(eif_url.as_str(), job.to_string(), instance_type.as_str(), region.clone(), req_mem, req_vcpus).await;
                                    if let Err(err) = res {
                                        println!("job {job}: Instance launch failed, {err}");
                                        break 'event;
                                    }
                                    instance_id = res.unwrap();
                                }
                            }
                        }
                    }
                    // insolvency check
                    () = sleep(insolvency_duration) => {
                        // spin down instance
                        if instance_id.as_str() != "" {
                            let res = aws_manager_impl.spin_down(&instance_id, region.clone()).await;
                            if let Err(err) = res {
                                println!("job {job}: ERROR failed to terminate instance, {err}");
                                break 'event;
                            }
                        }
                        println!("job {job}: INSOLVENCY: Spinning down instance");

                        // exit fully
                        break 'main;
                    }
                    // aws delayed spin up check
                    () = sleep(aws_delay_timeout) => {
                        let (exist, instance) = aws_manager_impl.get_job_instance(&job.to_string(), region.clone()).await.unwrap_or((false, "".to_string()));
                        if exist {
                            instance_id = instance;
                            println!("job {job}: Found, instance id: {instance_id}");
                            if rate < min_rate {
                                println!("job {job}: Rate below minimum, shutting down instance");
                                let res = aws_manager_impl.spin_down(&instance_id, region.clone()).await;
                                if let Err(err) = res {
                                    println!("job {job}: ERROR failed to terminate instance, {err}");
                                    break 'event;
                                }
                                instance_id = String::new();
                            }
                        } else if rate >= min_rate {
                            let res = aws_manager_impl.spin_up(eif_url.as_str(), job.to_string(), instance_type.as_str(), region.clone(), req_mem, req_vcpus).await;
                            if let Err(err) = res {
                                println!("job {job}: Instance launch failed, {err}");
                                break 'event;
                            }
                            instance_id = res.unwrap();
                        } else {
                            println!("job {job}: Rate below minimum, aborting launch.");
                        }
                        aws_launch_scheduled = false;
                    }
                    log = job_stream.next() => {
                        if log.is_none() { break 'event; }
                        let log = log.unwrap();
                        println!("job {}: New log: {}, {}", job, log.topics[0], log.data);

                        if log.topics[0] == JOB_OPENED {
                            // decode
                            if let Ok((metadata, _rate, _balance, timestamp)) = <(String, U256, U256, U256)>::decode(&log.data) {
                                // update solvency metrics
                                balance = _balance;
                                rate = _rate;
                                original_rate = _rate;
                                last_settled = Duration::from_secs(timestamp.low_u64());
                                println!("job {}: OPENED: metadata: {}, rate: {}, balance: {}, timestamp: {}", job, metadata, rate, balance, last_settled.as_secs());
                                let v = serde_json::from_str(&metadata);
                                if let Err(err) = v {
                                    println!("job {job}: Error reading metadata: {err}");
                                    break 'main;
                                }

                                let v: Value = v.unwrap();

                                let r = v["instance"].as_str();
                                match r {
                                    Some(t) => {
                                        instance_type = t.to_string();
                                        println!("job {job}: Instance type set: {instance_type}");
                                    }
                                    None => {
                                        println!("job {job}: Instance type not set");
                                        break 'main;
                                    }
                                }

                                let r = v["region"].as_str();
                                match r {
                                    Some(t) => {
                                        region = t.to_string();
                                        println!("job {job}: Job region set: {region}");
                                    }
                                    None => {
                                        println!("job {job}: Job region not set");
                                        break 'main;
                                    }
                                }

                                if !allowed_regions.contains(&region) {
                                    println!("job {job}: region : {region} not suppported, exiting job");
                                    break 'main;
                                }

                                let r = v["memory"].as_i64();
                                match r {
                                    Some(t) => {
                                        req_mem = t;
                                        println!("job {job}: Required memory: {req_mem}");
                                    }
                                    None => {
                                        println!("job {job}: memory not set");
                                        break 'main;
                                    }
                                }

                                let r = v["vcpu"].as_i64();
                                match r {
                                    Some(t) => {
                                        req_vcpus = t.try_into().unwrap_or(2);
                                        println!("job {job}: Required vcpu: {req_vcpus}");
                                    }
                                    None => {
                                        println!("job {job}: vcpu not set");
                                        break 'main;
                                    }
                                }

                                let url = v["url"].as_str();
                                if url.is_none() {
                                    println!("job {job}: eif url not found! Exiting job");
                                    break 'main;
                                }
                                eif_url = url.unwrap().to_string();

                                let file_path = rates_path.clone();
                                let contents = fs::read_to_string(file_path);

                                if let Err(err) = contents {
                                    println!("job {job}: Error reading rates file : {err}");
                                    break 'main;
                                } else {
                                    let contents = contents.unwrap();
                                    let data : Vec<server::RegionalRates> = serde_json::from_str(&contents).unwrap_or_default();
                                    let mut supported = false;
                                    for entry in data {
                                        if entry.region == region {
                                            for card in entry.rate_cards {
                                                if card.instance == instance_type {
                                                    min_rate = U256::from(card.min_rate);
                                                    supported = true;
                                                    break;
                                                }
                                            }
                                            break;
                                        }
                                    }
                                    if !supported {
                                        println!("job {job}: instance type {instance_type}, not supported");
                                        break 'main;
                                    }
                                }
                                println!("job {job}: MIN RATE for {instance_type} instance is {min_rate}");

                                aws_launch_time = Instant::now().checked_add(Duration::from_secs(aws_delay_duration)).unwrap();
                                aws_launch_scheduled = true;
                                println!("job {job}: Instance scheduled");
                            } else {
                                println!("job {}: OPENED: Decode failure: {}", job, log.data);
                            }
                        } else if log.topics[0] == JOB_SETTLED {
                            // decode
                            if let Ok((amount, timestamp)) = <(U256, U256)>::decode(&log.data) {
                                // update solvency metrics
                                balance -= amount;
                                last_settled = Duration::from_secs(timestamp.low_u64());
                                println!("job {}: SETTLED: amount: {}, rate: {}, balance: {}, timestamp: {}", job, amount, rate, balance, last_settled.as_secs());
                            } else {
                                println!("job {}: SETTLED: Decode failure: {}", job, log.data);
                            }
                        } else if log.topics[0] == JOB_CLOSED {
                            if !aws_launch_scheduled && instance_id.as_str() != "" {
                                let res = aws_manager_impl.spin_down(&instance_id, region.clone()).await;
                                if let Err(err) = res {
                                    println!("job {job}: ERROR failed to terminate instance, {err}");
                                    break 'event;
                                }
                                println!("job {job}: CLOSED: Spinning down instance");
                            } else {
                                println!("job {job}: Cancelled scheduled instance");
                            }
                            // exit fully
                            println!("job {job}: CLOSED");
                            break 'main;
                        } else if log.topics[0] == JOB_DEPOSITED {
                            // decode
                            if let Ok(amount) = U256::decode(&log.data) {
                                // update solvency metrics
                                balance += amount;
                                println!("job {}: DEPOSITED: amount: {}, rate: {}, balance: {}, timestamp: {}", job, amount, rate, balance, last_settled.as_secs());
                            } else {
                                println!("job {}: DEPOSITED: Decode failure: {}", job, log.data);
                            }
                        } else if log.topics[0] == JOB_WITHDREW {
                            // decode
                            if let Ok(amount) = U256::decode(&log.data) {
                                // update solvency metrics
                                balance -= amount;
                                println!("job {}: WITHDREW: amount: {}, rate: {}, balance: {}, timestamp: {}", job, amount, rate, balance, last_settled.as_secs());
                            } else {
                                println!("job {}: WITHDREW: Decode failure: {}", job, log.data);
                            }
                        } else if log.topics[0] == JOB_REVISE_RATE_INITIATED {
                            if let Ok(new_rate) = U256::decode(&log.data) {
                                original_rate = rate;
                                rate = new_rate;
                                if rate < min_rate {
                                    if aws_launch_scheduled {
                                        aws_launch_scheduled = false;
                                        println!("job {job}: Canelled scheduled instance");
                                    } else if instance_id.as_str() != ""{
                                        let res = aws_manager_impl.spin_down(&instance_id, region.clone()).await;
                                        if let Err(err) = res {
                                            println!("job {job}: ERROR failed to terminate instance, {err}");
                                            break 'event;
                                        }
                                        instance_id = String::new();
                                    }
                                    println!("job {job}: Revised job rate below min rate, shut down");
                                }
                                println!("job {}: JOB_REVISE_RATE_INTIATED: original_rate: {}, rate: {}, balance: {}, timestamp: {}", job, original_rate, rate, balance, last_settled.as_secs());
                            } else {
                                println!("job {}: JOB_REVISE_RATE_INITIATED: Decode failure: {}", job, log.data);
                            }
                        } else if log.topics[0] == JOB_REVISE_RATE_CANCELLED {
                            rate = original_rate;
                            if rate >= min_rate && !aws_launch_scheduled && instance_id.as_str() == ""{
                                aws_launch_scheduled = true;
                                aws_launch_time = Instant::now().checked_add(Duration::from_secs(aws_delay_duration)).unwrap();
                                println!("job {job}: Instance scheduled");
                            }
                            println!("job {}: JOB_REVISED_RATE_CANCELLED: rate: {}, balance: {}, timestamp: {}", job, rate, balance, last_settled.as_secs());
                        } else if log.topics[0] == JOB_REVISE_RATE_FINALIZED {
                            if let Ok(new_rate) = U256::decode(&log.data) {
                                if rate != new_rate {
                                    println!("Job {job}: Something went wrong, finalized rate not same as initiated rate");
                                    break 'main;
                                }
                                if rate >= min_rate && !aws_launch_scheduled && instance_id.as_str() == "" {
                                    aws_launch_scheduled = true;
                                    aws_launch_time = Instant::now().checked_add(Duration::from_secs(aws_delay_duration)).unwrap();
                                    println!("job {job}: Instance scheduled");
                                }
                                println!("job {}: JOB_REVISE_RATE_FINALIZED: original_rate: {}, rate: {}, balance: {}, timestamp: {}", job, original_rate, rate, balance, last_settled.as_secs());
                                original_rate = new_rate;
                            } else {
                                println!("job {}: JOB_REVISE_RATE_FINALIZED: Decode failure: {}", job, log.data);
                            }
                        } else {
                            println!("job {}: Unknown event: {}", job, log.topics[0]);
                        }
                    }
                }
            }

            println!("job {job}: Job stream ended");
        }
    }

    async fn job_logs(
        client: &Provider<Ws>,
        job: H256,
    ) -> Result<Box<dyn Stream<Item = Log> + Send + '_>, Box<dyn Error + Send + Sync + '_>> {
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
}

#[derive(Clone)]
pub struct TestLogger {}

#[async_trait]
impl Logger for TestLogger {
    async fn new_jobs<'a>(
        &'a self,
        _client: &'a Provider<Ws>,
    ) -> Result<Box<dyn Stream<Item = (H256, bool)> + 'a>, Box<dyn Error + Send + Sync + 'a>> {
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
    ) -> Result<Box<dyn Stream<Item = Log> + Send + 'a>, Box<dyn Error + Send + Sync + 'a>> {
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
impl AwsManager for TestAws {
    async fn spin_up(
        &mut self,
        eif_url: &str,
        job: String,
        instance_type: &str,
        region: String,
        req_mem: i64,
        req_vcpu: i32,
    ) -> Result<String, Box<dyn Error + Send + Sync>> {
        if self.outfile.as_str() != "" {
            let mut file = OpenOptions::new()
                .append(true)
                .open(&self.outfile)
                .expect("Unable to open out file");
            file.write_all("SpinUp\n".as_bytes()).expect("write failed");
        }
        println!(
            "TEST: spin_up | job: {job}, region: {region}, instance_type: {instance_type}, eif_url: {eif_url}, mem: {req_mem}, vcpu: {req_vcpu}"
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
        println!("TEST: spin_down | instance_id: {instance_id}, region: {region}");
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
    ) -> Result<(bool, String), Box<dyn Error + Send + Sync>> {
        println!("TEST: get_job_instance | job: {job}, region: {region}");
        Ok((false, "".to_string()))
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
    use ethers::prelude::*;
    use whoami::username;

    #[tokio::test]
    async fn test_1() {
        market::JobsService::job_manager(
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
            "/home/".to_owned() + &username() + "/.marlin/rates.json",
        )
        .await;
    }

    #[tokio::test]
    async fn test_2() {
        market::JobsService::job_manager(
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
            "/home/".to_owned() + &username() + "/.marlin/rates.json",
        )
        .await;
    }

    #[tokio::test]
    async fn test_3() {
        market::JobsService::job_manager(
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
            "/home/".to_owned() + &username() + "/.marlin/rates.json",
        )
        .await;
    }

    #[tokio::test]
    async fn test_4() {
        market::JobsService::job_manager(
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
            "/home/".to_owned() + &username() + "/.marlin/rates.json",
        )
        .await;
    }

    #[tokio::test]
    async fn test_5() {
        market::JobsService::job_manager(
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
            "/home/".to_owned() + &username() + "/.marlin/rates.json",
        )
        .await;
    }

    #[tokio::test]
    async fn test_6() {
        market::JobsService::job_manager(
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
            "/home/".to_owned() + &username() + "/.marlin/rates.json",
        )
        .await;
    }

    #[tokio::test]
    async fn test_7() {
        market::JobsService::job_manager(
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
            "/home/".to_owned() + &username() + "/.marlin/rates.json",
        )
        .await;
    }

    #[tokio::test]
    async fn test_8() {
        market::JobsService::job_manager(
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
            "/home/".to_owned() + &username() + "/.marlin/rates.json",
        )
        .await;
    }

    #[tokio::test]
    async fn test_9() {
        market::JobsService::job_manager(
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
            "/home/".to_owned() + &username() + "/.marlin/rates.json",
        )
        .await;
    }

    #[tokio::test]
    async fn test_10() {
        market::JobsService::job_manager(
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
            "/home/".to_owned() + &username() + "/.marlin/rates.json",
        )
        .await;
    }

    #[tokio::test]
    async fn test_11() {
        market::JobsService::job_manager(
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
            "/home/".to_owned() + &username() + "/.marlin/rates.json",
        )
        .await;
    }

    #[tokio::test]
    async fn test_12() {
        market::JobsService::job_manager(
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
            "/home/".to_owned() + &username() + "/.marlin/rates.json",
        )
        .await;
    }

    #[tokio::test]
    async fn test_13() {
        market::JobsService::job_manager(
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
            "/home/".to_owned() + &username() + "/.marlin/rates.json",
        )
        .await;
    }

    #[tokio::test]
    async fn test_14() {
        market::JobsService::job_manager(
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
            "/home/".to_owned() + &username() + "/.marlin/rates.json",
        )
        .await;
    }

    #[tokio::test]
    async fn test_15() {
        market::JobsService::job_manager(
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
            "/home/".to_owned() + &username() + "/.marlin/rates.json",
        )
        .await;
    }
}
