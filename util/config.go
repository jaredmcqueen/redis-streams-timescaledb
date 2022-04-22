package util

import "github.com/spf13/viper"

type Config struct {
	RedisEndpoint         string `mapstructure:"REDIS_ENDPOINT"`
	RedisStreamStart      string `mapstructure:"REDIS_STREAM_START"`
	TimescaleDBBatchSize  int64  `mapstructure:"TIMESCALEDB_BATCH_SIZE"`
	TimescaleDBConnection string `mapstructure:"TIMESCALEDB_CONNECTION"`
	TimescaleDBWorkers    int    `mapstructure:"TIMESCALEDB_WORKERS"`
}

// LoadConfig loads app.env if it exists and sets envars
func LoadConfig(path string) (config Config, err error) {
	viper.AddConfigPath(path)
	viper.SetConfigName("app")
	viper.SetConfigType("env")

	viper.AutomaticEnv()

	err = viper.ReadInConfig()
	if err != nil {
		return
	}

	err = viper.Unmarshal(&config)
	return
}
