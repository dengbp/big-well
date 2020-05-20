package com.yr.connector;

import com.yr.connector.bulk.BulkProcessor;
import com.yr.kudu.utils.KuduUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
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
    public static final String TOPIC_TABLE_MAP = "topic.table.map";
    public static final String KUDU_MASTERS = "kudu.masters";
    public static final String TABLE_LIST = "table.list";

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
                "Flush Timeout (ms)"
        ).define(
                MAX_BUFFERED_RECORDS_CONFIG,
                Type.INT,
                8000,
                Importance.HIGH,
                FLUSH_TIMEOUT_MS_DOC,
                group,
                ++order,
                Width.LONG,
                "Flush Timeout (ms)"
        ).define(
                LINGER_MS_CONFIG,
                Type.LONG,
                1000L,
                Importance.HIGH,
                FLUSH_TIMEOUT_MS_DOC,
                group,
                ++order,
                Width.LONG,
                "Flush Timeout (ms)"
        ).define(
                MAX_RETRIES_CONFIG,
                Type.INT,
                3,
                Importance.HIGH,
                FLUSH_TIMEOUT_MS_DOC,
                group,
                ++order,
                Width.SHORT,
                "Flush Timeout (ms)"
        ).define(
                RETRY_BACKOFF_MS_CONFIG,
                Type.LONG,
                1000L,
                Importance.HIGH,
                FLUSH_TIMEOUT_MS_DOC,
                group,
                ++order,
                Width.LONG,
                "Flush Timeout (ms)"
        ).define(
                FLUSH_TIMEOUT_MS_CONFIG,
                Type.LONG,
                10000L,
                Importance.HIGH,
                FLUSH_TIMEOUT_MS_DOC,
                group,
                ++order,
                Width.SHORT,
                "Flush Timeout (ms)"
        ).define(
                TABLE_LIST,
                Type.STRING,
                "",
                Importance.HIGH,
                FLUSH_TIMEOUT_MS_DOC,
                group,
                ++order,
                Width.SHORT,
                "Flush Timeout (ms)"
        ).define(
                KUDU_MASTERS,
                Type.STRING,
                "",
                Importance.HIGH,
                FLUSH_TIMEOUT_MS_DOC,
                group,
                ++order,
                Width.SHORT,
                "Flush Timeout (ms)"
        ).define(
                TOPIC_TABLE_MAP,
                Type.STRING,
                "",
                Importance.HIGH,
                FLUSH_TIMEOUT_MS_DOC,
                group,
                ++order,
                Width.SHORT,
                "Flush Timeout (ms)"
        ).define(
                BEHAVIOR_ON_MALFORMED_CONFIG,
                Type.STRING,
                BulkProcessor.BehaviorOnException.DEFAULT.toString(),
                Importance.LOW,
                BulkProcessor.BehaviorOnException.VALIDATOR.toString(),
                group,
                ++order,
                Width.SHORT,
                "Flush Timeout (ms)"
        );
    }

    public KuduSinkConnectorConfig(Map<?, ?> originals) {
        super(baseConfigDef(), originals);
    }
}
