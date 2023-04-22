use ethers::abi::AbiEncode;
use ethers::prelude::*;
use ethers::types::Log;
use ethers::utils::keccak256;
use std::str::FromStr;
use std::time::SystemTime;

enum Actions {
    Open,       // metadata(region, url, instance), rate, balance, timestamp
    Close,      //
    Settle,     // amount, timestamp
    Deposit,    // amount
    Withdraw,   // amount
    LockCreate, // new_rate, 0
    LockDelete, //
    ReviseRate, //
}

fn get_logs_data() -> Vec<(Actions, Bytes, U256)> {
    let mut idx: i128 = 0;
    let time = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let input = vec![
        // test : 1 -> job open and close
        (Actions::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://drive.google.com/file/d/1ADnr8vFo3vMlKCxc5KxQKtu5_nnreIBD/view?usp=sharing\",\"instance\":\"c6a.xlarge\"}".to_string(),2,1001,time+1).encode()),
        (Actions::Close, [].into()),

        // test : 2 -> deposit
        (Actions::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://drive.google.com/file/d/1ADnr8vFo3vMlKCxc5KxQKtu5_nnreIBD/view?usp=sharing\",\"instance\":\"c6a.xlarge\"}".to_string(),2,1001,time+3).encode()),
        (Actions::Deposit, (500).encode()),
        (Actions::Close, [].into()),

        // test : 3 -> withdraw
        (Actions::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://drive.google.com/file/d/1ADnr8vFo3vMlKCxc5KxQKtu5_nnreIBD/view?usp=sharing\",\"instance\":\"c6a.xlarge\"}".to_string(),2,1001,time+6).encode()),
        (Actions::Withdraw, (500).encode()),
        (Actions::Close, [].into()),

        // test : 4 -> settle
        (Actions::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://drive.google.com/file/d/1ADnr8vFo3vMlKCxc5KxQKtu5_nnreIBD/view?usp=sharing\",\"instance\":\"c6a.xlarge\"}".to_string(),2,1001,time+9).encode()),
        (Actions::Settle, (2, time+10).encode()),
        (Actions::Close, [].into()),

        // test : 5 -> revise rate
        (Actions::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://drive.google.com/file/d/1ADnr8vFo3vMlKCxc5KxQKtu5_nnreIBD/view?usp=sharing\",\"instance\":\"c6a.xlarge\"}".to_string(),2,1001,time+12).encode()),
        (Actions::LockCreate, (5,0).encode()),
        (Actions::LockDelete, [].into()),
        (Actions::ReviseRate, [].into()),
        (Actions::Close, [].into()),

        // test : 6 -> revise rate cancel
        (Actions::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://drive.google.com/file/d/1ADnr8vFo3vMlKCxc5KxQKtu5_nnreIBD/view?usp=sharing\",\"instance\":\"c6a.xlarge\"}".to_string(),2,1001,time+17).encode()),
        (Actions::LockCreate, (5,0).encode()),
        (Actions::LockDelete, [].into()),
        (Actions::Close, [].into()),

        // test : 7 -> region type not supported
        (Actions::Open, ("{\"region\":\"ap-east-2\",\"url\":\"https://drive.google.com/file/d/1ADnr8vFo3vMlKCxc5KxQKtu5_nnreIBD/view?usp=sharing\",\"instance\":\"c6a.xlarge\"}".to_string(),2,1001,time+21).encode()),
        (Actions::Close, [].into()),

        // test : 8 -> region not provided
        (Actions::Open, ("{\"url\":\"https://drive.google.com/file/d/1ADnr8vFo3vMlKCxc5KxQKtu5_nnreIBD/view?usp=sharing\",\"instance\":\"c6a.xlarge\"}".to_string(),2,1001,time+23).encode()),
        (Actions::Close, [].into()),

        // test : 9 -> instance type not provided
        (Actions::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://drive.google.com/file/d/1ADnr8vFo3vMlKCxc5KxQKtu5_nnreIBD/view?usp=sharing\"}".to_string(),2,1001,time+25).encode()),
        (Actions::Close, [].into()),

        // test : 10 -> instance type not supported
        (Actions::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://drive.google.com/file/d/1ADnr8vFo3vMlKCxc5KxQKtu5_nnreIBD/view?usp=sharing\",\"instance\":\"c6a.vsmall\"}".to_string(),2,1001,time+27).encode()),
        (Actions::Close, [].into()),

        // test : 11 -> eif url not provided
        (Actions::Open, ("{\"region\":\"ap-south-1\",\"instance\":\"c6a.xlarge\"}".to_string(),2,1001,time+29).encode()),
        (Actions::Close, [].into()),

        // test : 12 -> rate lower than min rate
        (Actions::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://drive.google.com/file/d/1ADnr8vFo3vMlKCxc5KxQKtu5_nnreIBD/view?usp=sharing\",\"instance\":\"c6a.large\"}".to_string(),2,1001,time+31).encode()),
        (Actions::Close, [].into()),

        // test : 13 -> rate higher than balance
        (Actions::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://drive.google.com/file/d/1ADnr8vFo3vMlKCxc5KxQKtu5_nnreIBD/view?usp=sharing\",\"instance\":\"c6a.xlarge\"}".to_string(),50,49,time+33).encode()),
        (Actions::Close, [].into()),

        // test : 14 -> withdraw to amount lower than rate
        (Actions::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://drive.google.com/file/d/1ADnr8vFo3vMlKCxc5KxQKtu5_nnreIBD/view?usp=sharing\",\"instance\":\"c6a.xlarge\"}".to_string(),20,1001,time+35).encode()),
        (Actions::Withdraw, (990).encode()),
        (Actions::Close, [].into()),

        // test : 15 -> revised rate lower than min rate and again to higher
        (Actions::Open, ("{\"region\":\"ap-south-1\",\"url\":\"https://drive.google.com/file/d/1ADnr8vFo3vMlKCxc5KxQKtu5_nnreIBD/view?usp=sharing\",\"instance\":\"c6a.large\"}".to_string(),5,1001,time+38).encode()),
        (Actions::LockCreate, (3,0).encode()),
        (Actions::LockDelete, [].into()),
        (Actions::ReviseRate, [].into()),
        (Actions::LockCreate, (8,0).encode()),
        (Actions::LockDelete, [].into()),
        (Actions::ReviseRate, [].into()),
        (Actions::Close, [].into()),
    ];
    let mut res: Vec<(Actions, Bytes, U256)> = Vec::new();
    for v in input {
        if let Actions::Open = v.0 {
            idx += 1;
        }
        res.push(get_data_tuple(v.0, v.1, idx));
    }

    res
}

fn get_data_tuple(action: Actions, data: Vec<u8>, job: i128) -> (Actions, Bytes, U256) {
    (
        action,
        Bytes::from(data),
        U256::from_dec_str(&job.to_string()).unwrap_or(U256::one()),
    )
}

pub fn test_logs() -> Vec<Log> {
    let data_logs = get_logs_data();
    let mut logs: Vec<Log> = Vec::new();

    for l in data_logs {
        let log = get_log(l.0, l.1, H256::from_uint(&l.2));
        logs.push(log);
    }

    logs
}

fn get_log(topic: Actions, data: Bytes, idx: H256) -> Log {
    let mut log = Log {
        address: H160::from_str("0x0F5F91BA30a00bD43Bd19466f020B3E5fc7a49ec").unwrap(),
        removed: Some(false),
        data,
        ..Default::default()
    };
    match topic {
        Actions::Open => {
            log.topics = vec![
                H256::from(keccak256(
                    "JobOpened(bytes32,string,address,address,uint256,uint256,uint256)",
                )),
                idx,
            ];
        }
        Actions::Close => {
            log.topics = vec![H256::from(keccak256("JobClosed(bytes32)")), idx];
        }
        Actions::Settle => {
            log.topics = vec![
                H256::from(keccak256("JobSettled(bytes32,uint256,uint256)")),
                idx,
            ];
        }
        Actions::Deposit => {
            log.topics = vec![
                H256::from(keccak256("JobDeposited(bytes32,address,uint256)")),
                idx,
            ];
        }
        Actions::Withdraw => {
            log.topics = vec![
                H256::from(keccak256("JobWithdrew(bytes32,address,uint256)")),
                idx,
            ];
        }
        Actions::LockCreate => {
            log.topics = vec![
                H256::from(keccak256("LockCreated(bytes32,bytes32,uint256,uint256)")),
                idx,
            ];
        }
        Actions::LockDelete => {
            log.topics = vec![
                H256::from(keccak256("LockDeleted(bytes32,bytes32,uint256)")),
                idx,
            ];
        }
        Actions::ReviseRate => {
            log.topics = vec![
                H256::from(keccak256("JobRevisedRate(bytes32,uint256)")),
                idx,
            ];
        }
    }

    log
}
