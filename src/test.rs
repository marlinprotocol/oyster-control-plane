use ethers::prelude::*;
use ethers::types::Log;
use ethers::utils::keccak256;
use std::str::FromStr;
use ethers::abi::AbiEncode;
use std::time::SystemTime;

enum actions {
    Open, // metadata(region, url, instance), rate, balance, timestamp
    Close, // 
    Settle, // amount, timestamp
    Deposit, // amount
    Withdraw, // amount
    LockCreate, // new_rate, 0
    LockDelete, // 
    ReviseRate //
}


fn get_logs_data() -> Vec<(actions, Bytes, U256)> {
    let mut idx = U256::one();
    let mut time = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap().as_secs();
    vec![
        // test : 1 -> job open and close
        (actions::Open, Bytes::from(("{\"region\":\"ap-south-1\",\"url\":\"https://drive.google.com/file/d/1ADnr8vFo3vMlKCxc5KxQKtu5_nnreIBD/view?usp=sharing\",\"instance\":\"c6a.xlarge\"}".to_string(),2,1001,time+1).encode()), U256::from_dec_str("1").unwrap_or(U256::one())),
        (actions::Close, Bytes::new(), U256::from_dec_str("1").unwrap_or(U256::one()))

        // test : 2 -> 
    ]
}

pub fn test_logs() -> Vec<Log> {
    

    let data_logs = get_logs_data();
    let mut logs: Vec<Log> = Vec::new();

    for l in data_logs {
        let log = get_log(l.0, l.1, H256::from_uint(&l.2));
        logs.push(log);
    }

    return logs.into();
}


fn get_log(topic: actions, data: Bytes, idx: H256) -> Log {

    let mut log = Log::default();
    log.address = H160::from_str("0x3FA4718a2fd55297CD866E5a0dE6Bc75E2b777d1").unwrap();
    log.removed = Some(false);
    log.data = data;
    match topic {
        actions::Open => {
            log.topics = vec![H256::from(keccak256(
                "JobOpened(bytes32,string,address,address,uint256,uint256,uint256)",
            )), idx];
        },
        actions::Close => {
            log.topics = vec![H256::from(keccak256(
                "JobClosed(bytes32)",
            )), idx];
        },
        actions::Settle => {
            log.topics = vec![H256::from(keccak256(
                "JobSettled(bytes32,uint256,uint256)",
            )), idx];
        },
        actions::Deposit => {
            log.topics = vec![H256::from(keccak256(
                "JobDeposited(bytes32,address,uint256)",
            )), idx];
        },
        actions::Withdraw => {
            log.topics = vec![H256::from(keccak256(
                "JobWithdrew(bytes32,address,uint256)",
            )), idx];
        },
        actions::LockCreate => {
            log.topics = vec![H256::from(keccak256(
                "LockCreated(bytes32,bytes32,uint256,uint256)",
            )), idx];
        },
        actions::LockDelete => {
            log.topics = vec![H256::from(keccak256(
                "LockDeleted(bytes32,bytes32,uint256)",
            )), idx];
        },
        actions::ReviseRate => {
            log.topics = vec![H256::from(keccak256(
                "JobRevisedRate(bytes32,uint256)",
            )), idx];
        }
    }
    
    log
}

