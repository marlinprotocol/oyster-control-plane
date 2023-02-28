
# Control Plane
The control plane listens to Marketplace contract events and automatically manages infrastructure for Oyster enclaves.

A long-running node is required to run the control plane and the instructions below assume commands are being run inside such a node.

This tutorial assumes Ubuntu 20.4+. If you have an older Ubuntu or a different distro, commands might need modification before use.

 
## Preliminaries

### Setup AWS profiles using the AWS CLI
This setup requires you to setup a named profile using AWS CLI 

 - To install AWS CLI on your system please follow ["Installing or updating the latest version of the AWS CLI"](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
 - Next configure the AWS CLI and setup a named profile by following ["Configuring the AWS CLI"](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html)

### Install Rust
The following project requires Rust to compile. To install Rust, please follow the instructions provided by the [link](https://www.rust-lang.org/tools/install)
The program requires **rustc** version 1.67.0 (fc594f156 2023-01-24) or up.

### Setup Radicle
This project uses Radicle for version control and distribution. To setup **rad-cli** please follow the instructions provided by the [link](https://radicle.xyz/get-started.html).

### Setup minimum rates file
You can download the default minimum rates file by running 

    command to download rates.txt to ~/.marlin/ folder
This file can then be edited as per your wish to set minimum rates for each instance type.

## Setting up and running the project
To clone the project `cd` into the desired location and run

    rad clone rad://radicle.lsqtech.org/hnrkk4b3996ywswdx9ck1x9op1kqij5s59a8o && cd control-plane
next run the following commands to build the binary 

    cargo update

    cargo clean && cargo build --release
This will generate a binary in the `control-plane/target/release/control-plane` location.

You can then run this binary

    ./control-plane --profile <aws_profile> --key-name <key_pair_name> --rpc <rpc_url> --region <region1> --region <region2> ...
Here the `region` are the list of aws regions you want to allow to be used for enclave launches.
