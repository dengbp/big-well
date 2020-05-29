package com.yr.connector;

import com.yr.connector.bulk.BulkProcessor;
import com.yr.kudu.utils.KuduUtil;
import com.yr.kudu.utils.RetryUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author dengbp
 * @ClassName FileStreamSinkTask
 * @Description TODO
 * @date 2020-05-15 18:21
 */
@Slf4j
public class KuduSinkTask extends SinkTask {

    private KuduWriter writer;
    private KuduClient client;

    @Override
    public String version() {
        return new KuduSinkConnector().version();
    }

    /**
     * Description todo
     * @param props
     * @return void
     * @Author dengbp
     * @Date 14:46 2020-05-19
     **/

    @Override
    public void start(Map<String, String> props) {
        log.info("KuduSinkTask begin start...");
        KuduSinkConnectorConfig config = new KuduSinkConnectorConfig(props);
        long flushTimeoutMs =
                config.getLong(KuduSinkConnectorConfig.FLUSH_TIMEOUT_MS_CONFIG);
        int maxBufferedRecords =
                config.getInt(KuduSinkConnectorConfig.MAX_BUFFERED_RECORDS_CONFIG);
        int batchSize =
                config.getInt(KuduSinkConnectorConfig.BATCH_SIZE_CONFIG);
        long lingerMs =
                config.getLong(KuduSinkConnectorConfig.LINGER_MS_CONFIG);
        long retryBackoffMs =
                config.getLong(KuduSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG);
        int maxRetry =
                config.getInt(KuduSinkConnectorConfig.MAX_RETRIES_CONFIG);
        BulkProcessor.BehaviorOnException behaviorOnException =
                BulkProcessor.BehaviorOnException.forValue(
                        config.getString(KuduSinkConnectorConfig.BEHAVIOR_ON_MALFORMED_CONFIG)
                );

        long maxRetryBackoffMs =
                RetryUtil.computeRetryWaitTimeInMillis(maxRetry, retryBackoffMs);
        if (maxRetryBackoffMs > RetryUtil.MAX_RETRY_TIME_MS) {
            log.warn("This connector uses exponential backoff with jitter for retries, "
                            + "and using '{}={}' and '{}={}' results in an impractical but possible maximum "
                            + "backoff time greater than {} hours.",
                    KuduSinkConnectorConfig.MAX_RETRIES_CONFIG, maxRetry,
                    KuduSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG, retryBackoffMs,
                    TimeUnit.MILLISECONDS.toHours(maxRetryBackoffMs));
        }

        String kuduMasters = config.getString(KuduSinkConnectorConfig.KUDU_MASTERS);
        client = new KuduClient.KuduClientBuilder(kuduMasters).build();
        try {
            String sourceSinkTableMap = config.getString(KuduSinkConnectorConfig.SOURCE_SINK_MAP);
            KuduUtil.init(client,sourceSinkTableMap);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("初始化失败...");
        }
        KuduWriter.Builder builder = new KuduWriter.Builder(client)
                .setFlushTimeoutMs(flushTimeoutMs)
                .setMaxBufferedRecords(maxBufferedRecords)
                .setBatchSize(batchSize)
                .setLingerMs(lingerMs)
                .setRetryBackoffMs(retryBackoffMs)
                .setMaxRetry(maxRetry)
                .setTopicToTable(topicTableMap)
                .setBehaviorOnException(behaviorOnException);
        writer = builder.build();
        writer.start();
    }


    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        log.info("Putting {} records to Kudu", sinkRecords.size());
        writer.write(sinkRecords);
    }

    /**
     * Description offset.flush.timeout.ms
     * @param offsets	 offsets
     * @return void
     * @Author dengbp
     * @Date 18:55 2020-05-22
     **/

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        log.info("Flushing data to kudu with the following offsets: {}", offsets);
        super.flush(offsets);
        writer.flush();
    }

    @Override
    public void stop() {
        log.info("Stopping kuduSinkTask");
        if (writer != null) {
            writer.stop();
        }
        if (client != null) {
            try {
                client.close();
            } catch (KuduException e) {
                e.printStackTrace();
            }
        }
    }
}
