package main

import (
	"github.com/marlinprotocol/EnclaveProviderControlPlane/cmd"
	"github.com/marlinprotocol/EnclaveProviderControlPlane/logger"
	"github.com/marlinprotocol/EnclaveProviderControlPlane/util"
)

func main() {
	util.ENOK(cmd.RootCmd.Execute())
}

func init() {
	logger.SetupLog()
}
