package config

import (
	"fmt"
	"os"
	"strconv"
)

type EnvConfing struct {
	HostAddr      string
	RabbitConName string
	RabbitURL     string
	MaxRetries    int
}

func EnvConfigError(envName string) error {
	return fmt.Errorf("Error by loading env: %s\n", envName)
}

func LoadEnvConfig() (*EnvConfing, error) {
	hostAddr, ok := os.LookupEnv("HOST_ADDR")
	if !ok || hostAddr == "" {
		return &EnvConfing{}, EnvConfigError("HOST_ADDR")
	}
	rConName, ok := os.LookupEnv("RABBIT_CONNECTION_NAME")
	if !ok || rConName == "" {
		return &EnvConfing{}, EnvConfigError("RABBIT_CONNECTION_NAME")
	}
	rURL, ok := os.LookupEnv("RABBIT_URL")
	if !ok || rURL == "" {
		return &EnvConfing{}, EnvConfigError("RABBIT_URL")
	}
	maxRetriesStr, ok := os.LookupEnv("MAX_RETRIES")
	if !ok || maxRetriesStr == "" {
		return &EnvConfing{}, EnvConfigError("MAX_RETRIES")
	}
	maxRetries, _ := strconv.Atoi(maxRetriesStr)
	return &EnvConfing{
		HostAddr:      hostAddr,
		RabbitConName: rConName,
		RabbitURL:     rURL,
		MaxRetries:    maxRetries,
	}, nil
}
