package cmd

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
)

const (
	configFileName = "config"
	configFileType = "yaml"
)

var configDirOverride string

type Config struct {
	APIURL string `mapstructure:"api_url" json:"api_url" yaml:"api_url"`
	Token  string `mapstructure:"token" json:"token" yaml:"token"`
}

func configDir() (string, error) {
	if configDirOverride != "" {
		return configDirOverride, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".loadforge"), nil
}

func newViper() (*viper.Viper, error) {
	dir, err := configDir()
	if err != nil {
		return nil, err
	}
	v := viper.New()
	v.SetConfigName(configFileName)
	v.SetConfigType(configFileType)
	v.AddConfigPath(dir)
	v.SetDefault("api_url", "http://localhost:8080")
	v.SetEnvPrefix("loadforge")
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))
	if err := v.ReadInConfig(); err != nil {
		var notFound viper.ConfigFileNotFoundError
		if !errors.As(err, &notFound) {
			return nil, err
		}
	}
	return v, nil
}

func loadConfig() (Config, error) {
	v, err := newViper()
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func writeConfig(cfg Config) error {
	dir, err := configDir()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	v := viper.New()
	v.SetConfigFile(filepath.Join(dir, configFileName+"."+configFileType))
	v.SetConfigType(configFileType)
	v.Set("api_url", cfg.APIURL)
	v.Set("token", cfg.Token)
	return v.WriteConfigAs(filepath.Join(dir, configFileName+"."+configFileType))
}

func maskToken(token string) string {
	token = strings.TrimSpace(token)
	if token == "" {
		return ""
	}
	if len(token) <= 4 {
		return strings.Repeat("*", len(token))
	}
	return strings.Repeat("*", len(token)-4) + token[len(token)-4:]
}
