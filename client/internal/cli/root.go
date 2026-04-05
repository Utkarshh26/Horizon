package cli

import (
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "horizon",
	Short: "Horizon — expose localhost through the tunnel server",
}

// Execute runs the Cobra command tree.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.AddCommand(exposeCmd)
}
