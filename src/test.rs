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
    let mut idx: i128 = 1;
    let time = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap().as_secs();
    let input = vec![
        // test : 1 -> job open and close
        (actions::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://drive.google.com/file/d/1ADnr8vFo3vMlKCxc5KxQKtu5_nnreIBD/view?usp=sharing\",\"instance\":\"c6a.xlarge\"}".to_string(),2,1001,time+1).encode()),
        (actions::Close, [].into()),

        // test : 2 -> deposit
        (actions::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://drive.google.com/file/d/1ADnr8vFo3vMlKCxc5KxQKtu5_nnreIBD/view?usp=sharing\",\"instance\":\"c6a.xlarge\"}".to_string(),2,1001,time+3).encode()),
        (actions::Deposit, (500).encode()),
        (actions::Close, [].into()),

        // test : 3 -> withdraw
        (actions::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://drive.google.com/file/d/1ADnr8vFo3vMlKCxc5KxQKtu5_nnreIBD/view?usp=sharing\",\"instance\":\"c6a.xlarge\"}".to_string(),2,1001,time+6).encode()),
        (actions::Withdraw, (500).encode()),
        (actions::Close, [].into()),

        // test : 4 -> settle
        (actions::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://drive.google.com/file/d/1ADnr8vFo3vMlKCxc5KxQKtu5_nnreIBD/view?usp=sharing\",\"instance\":\"c6a.xlarge\"}".to_string(),2,1001,time+9).encode()),
        (actions::Settle, (2, time+10).encode()),
        (actions::Close, [].into()),

        // test : 5 -> revise rate
        (actions::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://drive.google.com/file/d/1ADnr8vFo3vMlKCxc5KxQKtu5_nnreIBD/view?usp=sharing\",\"instance\":\"c6a.xlarge\"}".to_string(),2,1001,time+12).encode()),
        (actions::LockCreate, (5,0).encode()),
        (actions::LockDelete, [].into()),
        (actions::ReviseRate, [].into()),
        (actions::Close, [].into()),

        // test : 6 -> revise rate cancel
        (actions::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://drive.google.com/file/d/1ADnr8vFo3vMlKCxc5KxQKtu5_nnreIBD/view?usp=sharing\",\"instance\":\"c6a.xlarge\"}".to_string(),2,1001,time+17).encode()),
        (actions::LockCreate, (5,0).encode()),
        (actions::LockDelete, [].into()),
        (actions::Close, [].into()),

        // test : 7 -> region type not supported
        (actions::Open, ("{\"region\":\"ap-east-2\",\"url\":\"https://drive.google.com/file/d/1ADnr8vFo3vMlKCxc5KxQKtu5_nnreIBD/view?usp=sharing\",\"instance\":\"c6a.xlarge\"}".to_string(),2,1001,time+21).encode()),
        (actions::Close, [].into()),

        // test : 8 -> region not provided
        (actions::Open, ("{\"url\":\"https://drive.google.com/file/d/1ADnr8vFo3vMlKCxc5KxQKtu5_nnreIBD/view?usp=sharing\",\"instance\":\"c6a.xlarge\"}".to_string(),2,1001,time+23).encode()),
        (actions::Close, [].into()),
        
        // test : 9 -> instance type not provided
        (actions::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://drive.google.com/file/d/1ADnr8vFo3vMlKCxc5KxQKtu5_nnreIBD/view?usp=sharing\"}".to_string(),2,1001,time+25).encode()),
        (actions::Close, [].into()),

        // test : 10 -> instance type not supported
        (actions::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://drive.google.com/file/d/1ADnr8vFo3vMlKCxc5KxQKtu5_nnreIBD/view?usp=sharing\",\"instance\":\"c6a.vsmall\"}".to_string(),2,1001,time+27).encode()),
        (actions::Close, [].into()),

        // test : 11 -> eif url not provided
        (actions::Open, ("{\"region\":\"ap-south-1\",\"instance\":\"c6a.xlarge\"}".to_string(),2,1001,time+29).encode()),
        (actions::Close, [].into()),

        // test : 12 -> rate lower than min rate
        (actions::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://drive.google.com/file/d/1ADnr8vFo3vMlKCxc5KxQKtu5_nnreIBD/view?usp=sharing\",\"instance\":\"c6a.large\"}".to_string(),2,1001,time+31).encode()),
        (actions::Close, [].into()),

        // test : 13 -> rate higher than balance
        (actions::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://drive.google.com/file/d/1ADnr8vFo3vMlKCxc5KxQKtu5_nnreIBD/view?usp=sharing\",\"instance\":\"c6a.xlarge\"}".to_string(),50,49,time+33).encode()),
        (actions::Close, [].into()),

        // test : 14 -> withdraw to amount lower than rate
        (actions::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://drive.google.com/file/d/1ADnr8vFo3vMlKCxc5KxQKtu5_nnreIBD/view?usp=sharing\",\"instance\":\"c6a.xlarge\"}".to_string(),20,1001,time+35).encode()),
        (actions::Withdraw, (990).encode()),
        (actions::Close, [].into()),

        // test : 15 -> revised rate lower than min rate and again to higher
        (actions::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://drive.google.com/file/d/1ADnr8vFo3vMlKCxc5KxQKtu5_nnreIBD/view?usp=sharing\",\"instance\":\"c6a.large\"}".to_string(),5,1001,time+38).encode()),
        (actions::LockCreate, (3,0).encode()),
        (actions::LockDelete, [].into()),
        (actions::ReviseRate, [].into()),
        (actions::LockCreate, (8,0).encode()),
        (actions::LockDelete, [].into()),
        (actions::ReviseRate, [].into()),
        (actions::Close, [].into()),
    ];
    let mut res: Vec<(actions, Bytes, U256)> = Vec::new();
    for v in input {
        match v.0 {
            actions::Open => {
                idx += 1;
            },
            _ => {

            }
        }
        res.push(get_data_tuple(v.0, v.1, idx));
    }

    res
}

fn get_data_tuple(action: actions, data: Vec<u8>, job: i128) -> (actions, Bytes, U256) {
    (action, Bytes::from(data), U256::from_dec_str(&job.to_string()).unwrap_or(U256::one()))
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

