package com.yr.connector;

import com.yr.connector.bulk.BulkProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import java.util.Map;

/**
 * @author dengbp
 * @ClassName KuduSinkConnectorConfig
 * @Description TODO
 * @date 2020-05-16 10:41
 */
@Slf4j
public class KuduSinkConnectorConfig  extends AbstractConfig {

    private static final String FLUSH_TIMEOUT_MS_DOC = "kudu config";
    public static final String BATCH_SIZE_CONFIG = "batch.size";
    public static final String MAX_BUFFERED_RECORDS_CONFIG = "max.buffered.records";
    public static final String LINGER_MS_CONFIG = "linger.ms";
    public static final String FLUSH_TIMEOUT_MS_CONFIG = "flush.timeout.ms";
    public static final String MAX_RETRIES_CONFIG = "max.retries";
    public static final String RETRY_BACKOFF_MS_CONFIG = "retry.backoff.ms";
    public static final String BEHAVIOR_ON_MALFORMED_CONFIG = "behavior.on.exception";
    public static final String KUDU_MASTERS = "kudu.masters";
    public static final String SOURCE_SINK_MAP = "source.sink.map";

    protected static final ConfigDef CONFIG = baseConfigDef();

    private static ConfigDef baseConfigDef() {
        final ConfigDef configDef = new ConfigDef();
        definePro(configDef);
        return configDef;
    }

    private static void definePro(ConfigDef configDef){
        final String group = "connector";
        int order = 0;
        configDef.define(
                BATCH_SIZE_CONFIG,
                Type.INT,
                5000,
                Importance.HIGH,
                FLUSH_TIMEOUT_MS_DOC,
                group,
                ++order,
                Width.LONG,
                "batch_size_config"
        ).define(
                MAX_BUFFERED_RECORDS_CONFIG,
                Type.INT,
                8000,
                Importance.HIGH,
                FLUSH_TIMEOUT_MS_DOC,
                group,
                ++order,
                Width.LONG,
                "max_buffered_records_config"
        ).define(
                LINGER_MS_CONFIG,
                Type.LONG,
                1000L,
                Importance.HIGH,
                FLUSH_TIMEOUT_MS_DOC,
                group,
                ++order,
                Width.LONG,
                "linger_ms_config"
        ).define(
                MAX_RETRIES_CONFIG,
                Type.INT,
                3,
                Importance.HIGH,
                FLUSH_TIMEOUT_MS_DOC,
                group,
                ++order,
                Width.SHORT,
                "max_retries_config"
        ).define(
                RETRY_BACKOFF_MS_CONFIG,
                Type.LONG,
                1000L,
                Importance.HIGH,
                FLUSH_TIMEOUT_MS_DOC,
                group,
                ++order,
                Width.LONG,
                "retry_backoff_ms_config"
        ).define(
                FLUSH_TIMEOUT_MS_CONFIG,
                Type.LONG,
                10000L,
                Importance.HIGH,
                FLUSH_TIMEOUT_MS_DOC,
                group,
                ++order,
                Width.SHORT,
                "flush_timeout_ms_config"
        ).define(
                KUDU_MASTERS,
                Type.STRING,
                "",
                Importance.HIGH,
                FLUSH_TIMEOUT_MS_DOC,
                group,
                ++order,
                Width.SHORT,
                "kudu_masters"
        ).define(
                SOURCE_SINK_MAP,
                Type.STRING,
                "",
                Importance.HIGH,
                "",
                group,
                ++order,
                Width.SHORT,
                "source_sink_map"
        ).define(
                BEHAVIOR_ON_MALFORMED_CONFIG,
                Type.STRING,
                BulkProcessor.BehaviorOnException.DEFAULT.toString(),
                Importance.LOW,
                BulkProcessor.BehaviorOnException.VALIDATOR.toString(),
                group,
                ++order,
                Width.SHORT,
                "behavior_on_malformed_config"
        );
    }

    public KuduSinkConnectorConfig(Map<?, ?> originals) {
        super(baseConfigDef(), originals);
    }
}
