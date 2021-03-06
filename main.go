package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

func main() {
	viper.SetDefault("LogLevel", "Debug")
	viper.SetDefault("DBDir", "./db")
	viper.SetDefault("GRPCPort", ":9090")
	viper.SetDefault("RocksDBThreads", 4)

	viper.SetConfigName("gcore-config")
	viper.AddConfigPath(".")    // optionally look for config in the working directory
	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		logrus.Warn("config.yaml not found, using default config")
	}
	lvl, err := logrus.ParseLevel(viper.GetString("LogLevel"))
	if err != nil { // Handle errors reading the config file
		logrus.Fatalf("logrus level is in invalid format: %v", err)
	}
	logrus.SetLevel(lvl)

	if lvl == logrus.DebugLevel {
		e := logrus.WithFields(logrus.Fields{})
		for k, v := range viper.AllSettings() {
			e = e.WithField(k, v)
		}
		e.Debug("config")
	}

	logrus.Info("starting server")
	r, err := NewSelectRuntime(viper.GetString("DBDir"))
	if err != nil {
		logrus.Fatal(err)
	}

	go func() {
		lis, err := net.Listen("tcp", viper.GetString("GRPCPort"))
		if err != nil {
			logrus.Fatalf("failed to listen: %v", err)
		}
		server := grpc.NewServer()
		RegisterRuntimeServer(server, &Server{r: r, procs: map[string]*Proc{}})
		err = server.Serve(lis)
		if err != nil {
			logrus.Fatal(err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT)
		<-sigs
		cancel()
	}()

	err = r.Run(ctx)
	if err != nil {
		logrus.Fatalf("server run: %v", err)
	}
	logrus.Info("server shut down")
}
