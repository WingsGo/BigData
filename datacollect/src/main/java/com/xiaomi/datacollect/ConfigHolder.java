package com.xiaomi.datacollect;

import java.io.IOException;
import java.util.Properties;

public class ConfigHolder {
    private Properties properties = new Properties();
    private static ConfigHolder instance;

    private ConfigHolder() {
    }

    public static synchronized ConfigHolder getInstance() throws IOException {
        if (instance == null) {
            instance = new ConfigHolder();
            instance.properties.load(ConfigHolder.class.getClassLoader().getResourceAsStream("config.properties"));
        }
        return instance;
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }
}

