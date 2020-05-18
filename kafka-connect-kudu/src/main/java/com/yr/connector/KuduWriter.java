package com.yr.connector;

import com.yr.kudu.service.KuduOperat;
import com.yr.pojo.TableValues;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kudu.client.KuduException;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @author dengbp
 * @ClassName KuduWriter
 * @Description TODO
 * @date 2020-05-16 10:52
 */
@Slf4j
public class KuduWriter {

    private final KuduClient client;
    private final String type;
    private final boolean ignoreKey;
    private final Set<String> ignoreKeyTopics;
    private final boolean ignoreSchema;
    private final Set<String> ignoreSchemaTopics;
    private final Map<String, String> topicToTableMap;
    private final long flushTimeoutMs;
    private final BulkProcessor bulkProcessor;
    private final boolean dropInvalidMessage;
    /** topic——>table */
    private final Map<String,String> topicTableMap;
    private static final Converter jsonConverter;

    static {
        jsonConverter = new JsonConverter();
        jsonConverter.configure(Collections.singletonMap("schemas.enable", "false"), false);
    }


    private KuduWriter(KuduClient client, String type, boolean ignoreKey, Set<String> ignoreKeyTopics, boolean ignoreSchema, Set<String> ignoreSchemaTopics, Map<String, String> topicToTableMap, long flushTimeoutMs, boolean dropInvalidMessage, Map<String,String> topicTableMap) {
        this.client = client;
        this.type = type;
        this.ignoreKey = ignoreKey;
        this.ignoreKeyTopics = ignoreKeyTopics;
        this.ignoreSchema = ignoreSchema;
        this.ignoreSchemaTopics = ignoreSchemaTopics;
        this.topicToTableMap = topicToTableMap;
        this.flushTimeoutMs = flushTimeoutMs;
        this.topicTableMap = topicTableMap;
        this.bulkProcessor = new BulkProcessor();
        this.dropInvalidMessage = dropInvalidMessage;
    }

    public void start() {
        bulkProcessor.start();
    }

    public void write(Collection<SinkRecord> records) {
        for (SinkRecord record : records){
            String tableName = topicTableMap.get(record.topic());
            try {
                KuduOperat.insert(new TableValues(tableName,record.value().toString()));
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KuduException e) {
                e.printStackTrace();
                log.error("同步异常数据：{}",record.toString());
            }
        }
    }

    public void flush() {
        bulkProcessor.flush(flushTimeoutMs);
    }

    public void stop() {
        bulkProcessor.flush(flushTimeoutMs);
        bulkProcessor.stop();
    }


    public static class Builder {
        private final KuduClient client;
        private String type;
        private boolean useCompactMapEntries = true;
        private boolean ignoreKey = false;
        private Set<String> ignoreKeyTopics = Collections.emptySet();
        private boolean ignoreSchema = false;
        private Set<String> ignoreSchemaTopics = Collections.emptySet();
        private Map<String, String> topicToTableMap = new HashMap<>();
        private long flushTimeoutMs;
        private int maxBufferedRecords;
        private int maxInFlightRequests;
        private int batchSize;
        private long lingerMs;
        private int maxRetry;
        private long retryBackoffMs;
        private boolean dropInvalidMessage;

        public Builder(KuduClient client) {
            this.client = client;
        }

        public Builder setType(String type) {
            this.type = type;
            return this;
        }

        public Builder setIgnoreKey(boolean ignoreKey, Set<String> ignoreKeyTopics) {
            this.ignoreKey = ignoreKey;
            this.ignoreKeyTopics = ignoreKeyTopics;
            return this;
        }

        public Builder setIgnoreSchema(boolean ignoreSchema, Set<String> ignoreSchemaTopics) {
            this.ignoreSchema = ignoreSchema;
            this.ignoreSchemaTopics = ignoreSchemaTopics;
            return this;
        }

        public Builder setCompactMapEntries(boolean useCompactMapEntries) {
            this.useCompactMapEntries = useCompactMapEntries;
            return this;
        }



        public Builder setFlushTimoutMs(long flushTimeoutMs) {
            this.flushTimeoutMs = flushTimeoutMs;
            return this;
        }

        public Builder setMaxBufferedRecords(int maxBufferedRecords) {
            this.maxBufferedRecords = maxBufferedRecords;
            return this;
        }

        public Builder setMaxInFlightRequests(int maxInFlightRequests) {
            this.maxInFlightRequests = maxInFlightRequests;
            return this;
        }

        public Builder setBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder setLingerMs(long lingerMs) {
            this.lingerMs = lingerMs;
            return this;
        }

        public Builder setMaxRetry(int maxRetry) {
            this.maxRetry = maxRetry;
            return this;
        }

        public Builder setRetryBackoffMs(long retryBackoffMs) {
            this.retryBackoffMs = retryBackoffMs;
            return this;
        }

        public Builder setDropInvalidMessage(boolean dropInvalidMessage) {
            this.dropInvalidMessage = dropInvalidMessage;
            return this;
        }


        public KuduWriter build() {
            String[] tableMaps = client.getProps().get("topic.table.map").split(",");
            Map<String,String> map = new HashMap();
            for (String table : tableMaps){
                map.put(table.split(":")[0],table.split(":")[1]);
            }
            return new KuduWriter(client,type,ignoreKey, ignoreKeyTopics,ignoreSchema,
                    ignoreSchemaTopics, topicToTableMap,flushTimeoutMs,dropInvalidMessage,
                    map);
        }
    }
}
