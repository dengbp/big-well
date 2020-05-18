package com.yr.connector;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * @author dengbp
 * @ClassName KuduSinkConnectorConfig
 * @Description TODO
 * @date 2020-05-16 10:41
 */
@Slf4j
public class KuduSinkConnectorConfig  extends AbstractConfig {


    protected static final ConfigDef CONFIG = baseConfigDef();

    private static ConfigDef baseConfigDef() {
        final ConfigDef configDef = new ConfigDef();
        return configDef;
    }

    public KuduSinkConnectorConfig(Map<?, ?> originals) {
        super(baseConfigDef(), originals);
    }
}