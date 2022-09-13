package cmd

import (
	"runtime"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// RootCmd represents the base command when called without any subcommands
var ControlPlane = &cobra.Command{
	Use:   "ctrl",
	Short: "run enclave provider control plane",
	Long:  `run enclave provider control plane`,
	PreRun: func(cmd *cobra.Command, args []string) {
		runtime.GOMAXPROCS(1)
		log.Info("set runtime max parallelism: ", 1)
	},
	Run: StartControlPlane,
}

func StartControlPlane(cmd *cobra.Command, args []string) {
	log.Info("trying to connect to control plane persistence backend")
	// dbconn, err := db.SetupConnection()
	// util.ENOK(err)
	// mostRecent := dbconn.GetMostRecentPostedBlockHeight()

}
