// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2023 mochi-mqtt
// SPDX-FileContributor: dgduncan, mochi-co

package main

import (
	"flag"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/mochi-mqtt/server/v2/config"

	mqtt "github.com/mochi-mqtt/server/v2"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, nil))) // set basic logger to ensure logs before configuration are in a consistent format

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		done <- true
	}()

	addr := flag.String("conf", "/etc/v2k/platform/mq/config.yaml", "configuration file")
	flag.Parse()

	conf, ok := os.LookupEnv("CONFIG_PATH")

	if !ok {
		conf = *addr
	}

	configBytes, err := os.ReadFile(conf)

	if err != nil {
		log.Fatal(err)
	}

	options, err := config.FromBytes(configBytes)

	if err != nil {
		log.Fatal(err)
	}

	server := mqtt.New(options)

	go func() {
		if err := server.Serve(); err != nil {
			log.Fatal(err)
		}
	}()

	<-done
	server.Log.Warn("caught signal, stopping...")
	_ = server.Close()
	server.Log.Info("mochi mqtt shutdown complete")
}
