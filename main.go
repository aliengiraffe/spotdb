package main

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/aliengiraffe/spotdb/pkg/log"

	"flag"

	"github.com/aliengiraffe/spotdb/cmd/app"
	"github.com/aliengiraffe/spotdb/internal/vcs"
)

var version = vcs.Version()

func main() {

	displayVersion := flag.Bool("version", false, "Display version and exit")

	flag.Parse()

	if *displayVersion {
		fmt.Println(version) //nolint:forbidigo // needed to print out version
		os.Exit(0)
	}
	// Create a channel to listen for OS signals for graceful shutdown
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	l := log.GetLogger("spotdb")

	// Start our application in a goroutine
	l.Info("Starting SpotDB...")
	errCh := make(chan error, 1)
	go func() {
		if err := app.Run(l); err != nil {
			l.Error("Error starting application", slog.Any("error", err))
			errCh <- err
		}
	}()

	// Wait for signal or error
	select {
	case err := <-errCh:
		l.Error("Application failed", slog.Any("error", err))
	case sig := <-sigs:
		l.Info("Received signal", slog.Any("signal", sig))
		// Shutdown the application
		l.Info("Shutting down...")
		app.Shutdown(l)
		l.Info("Shutdown complete")
	}
}
