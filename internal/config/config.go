package config

type Config struct {

}

var (
	config *Config
	configOnce    sync.Once
)

func GetConfig() Config {
	if config == nil {
		panic("config not loaded")
	}
	return *config
}

func Setup(prefix string, name string, path string) error {
	viper.SetConfigName("config")
	viper.SetEnvPrefix(prefix)
	// Set the path to look for the configurations file
	viper.AddConfigPath(fmt.Sprintf("/etc/%s/", name))
	if path != "" {
		zap.L().Info("adding config path", zap.String("path", path))
		viper.AddConfigPath(path)
	}
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	// Enable VIPER to read Environment Variables
	viper.AutomaticEnv()
	viper.SetConfigType("yaml")

	err := viper.ReadInConfig()
	if err != nil {
		switch err.(type) {
		default:
			return errors.Errorf("fatal error loading config file: %v", err)
		case viper.ConfigFileNotFoundError:
			zap.L().Warn("No config file found. Using defaults and environment variables")
		}
	}
	return nil
}

// https://github.com/spf13/viper/issues/188 ideas for better config
func LoadConfig(path string) error {
	var err error
	configOnce.Do(func() {
		innerErr := Setup("dp", "devplane", path)
		if err != nil {
			err = errors.Join(err, innerErr)
			return
		}
		configuration := NewDefaultBackendConfig()
		BindEnvs(configuration)
		err = viper.Unmarshal(&configuration)
		if err != nil {
			err = errors.Join(err, innerErr)
			return
		}

		err = configuration.Validate()
		if err != nil {
			err = errors.Join(err, innerErr)
			return
		}

		backendConfig = &configuration
	})
	if err != nil {
		return errors.WrapAndTrace(err)
	}
	return nil
}
