package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	version     = "dev"
	commit      = "none"
	date        = "unknown"
	showVersion bool
)

var rootCmd = &cobra.Command{
	Use:   "spotdb",
	Short: "SpotDB - Package for running ephemeral data sandboxes enabled with DuckDB.",
	Run: func(cmd *cobra.Command, args []string) {
		if showVersion {
			fmt.Printf("Version: %s\n", version)
			fmt.Printf("Commit: %s\n", commit)
			fmt.Printf("Date: %s\n", date)
			return
		} else {
			_ = cmd.Help()
		}
	},
}

func init() {
	rootCmd.Flags().BoolVarP(&showVersion, "version", "v", false, "Show version information.")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
