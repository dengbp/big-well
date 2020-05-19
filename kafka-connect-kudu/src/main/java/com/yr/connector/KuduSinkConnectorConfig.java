package com.yr.connector;

import com.yr.kudu.utils.KuduUtil;
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

    public static final String CONNECTION_URL_CONFIG = "connection.url";
    public static final String CONNECTION_USERNAME_CONFIG = "connection.username";
    public static final String CONNECTION_PASSWORD_CONFIG = "connection.password";
    public static final String BATCH_SIZE_CONFIG = "batch.size";
    public static final String MAX_IN_FLIGHT_REQUESTS_CONFIG = "max.in.flight.requests";
    public static final String MAX_BUFFERED_RECORDS_CONFIG = "max.buffered.records";
    public static final String LINGER_MS_CONFIG = "linger.ms";
    public static final String FLUSH_TIMEOUT_MS_CONFIG = "flush.timeout.ms";
    public static final String MAX_RETRIES_CONFIG = "max.retries";
    public static final String RETRY_BACKOFF_MS_CONFIG = "retry.backoff.ms";
    public static final String KEY_IGNORE_CONFIG = "key.ignore";
    public static final String TOPIC_KEY_IGNORE_CONFIG = "topic.key.ignore";
    public static final String DROP_INVALID_MESSAGE_CONFIG = "drop.invalid.message";
    public static final String COMPACT_MAP_ENTRIES_CONFIG = "compact.map.entries";
    public static final String MAX_CONNECTION_IDLE_TIME_MS_CONFIG = "max.connection.idle.time.ms";
    public static final String CONNECTION_TIMEOUT_MS_CONFIG = "connection.timeout.ms";
    public static final String READ_TIMEOUT_MS_CONFIG = "read.timeout.ms";
    public static final String CONNECTION_COMPRESSION_CONFIG = "connection.compression";
    public static final String BEHAVIOR_ON_NULL_VALUES_CONFIG = "behavior.on.null.values";
    public static final String BEHAVIOR_ON_MALFORMED_DOCS_CONFIG = "behavior.on.exception";
    public static final String WRITE_METHOD_CONFIG = "write.method";
    public static final String TOPIC_TABLE_MAP = "topic.table.map";
    public static final String KUDU_MASTERS = "kudu.masters";
    public static final String TABLE_LIST = "table.list";

    protected static final ConfigDef CONFIG = baseConfigDef();

    private static ConfigDef baseConfigDef() {
        final ConfigDef configDef = new ConfigDef();
        return configDef;
    }

    public KuduSinkConnectorConfig(Map<?, ?> originals) {
        super(baseConfigDef(), originals);
    }
}
