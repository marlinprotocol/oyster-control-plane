package cmd

import (
	"fmt"

	"github.com/marlinprotocol/EnclaveProviderControlPlane/logger"
	"github.com/marlinprotocol/EnclaveProviderControlPlane/util"
	"github.com/marlinprotocol/EnclaveProviderControlPlane/version"
	"github.com/spf13/cobra"
)

var cfgFile string
var logLevel string

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:     "epcp",
	Short:   "epcp is marlin's enclave provider control plane",
	Long:    `epcp is marlin's enclave provider control plane`,
	Version: version.RootCmdVersion,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Incorrect invocation. See epcp --help for subcommands.")
	},
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		util.ENOK(logger.SetLogLevel(logLevel))

		if cfgFile == "" {
			cfgFile = util.GetUserHomedir() + "/.marlin/epcp/config.yaml"
		}
		// util.ENOK(config.LoadViperConfig(cfgFile))
		// util.ENOK(config.CheckViperMandatoryFields())
		// go instrumentation.StartPromServer()
	},
}

func init() {
	RootCmd.AddCommand(ControlPlane)

	RootCmd.PersistentFlags().StringVarP(&logLevel, "loglevel", "l", "info", "loglevel (default is INFO)")
	RootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file (default is $HOME/.marlin/epcp/config.yaml)")
}
