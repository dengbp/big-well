package com.yr.connector;

import com.yr.connector.bulk.BulkProcessor;
import com.yr.connector.bulk.KuduOperate;
import com.yr.kudu.session.SessionManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kudu.client.KuduClient;

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
    private final Map<String, String> topicToTableMap;
    /** 消息过期时间，如果消息缓存满了，在指定flushTimeoutMs之后无法放入就要报异常*/
    private final long flushTimeoutMs;
    private final BulkProcessor bulkProcessor;
    private final SessionManager sessionManager;

    public Map<String, String> getTopicTableMap() {
        return topicTableMap;
    }

    /** topic——>table */
    private final Map<String,String> topicTableMap;
    private final BulkProcessor.BehaviorOnException behaviorOnException;


    private KuduWriter(KuduClient client, Map<String, String> topicToTableMap, long flushTimeoutMs,
                       int maxBufferedRecords,
                       int batchSize,
                       long lingerMs,
                       int maxRetries,
                       long retryBackoffMs,Map<String, String> topicTableMap, BulkProcessor.BehaviorOnException behaviorOnException) {
        this.client = client;
        this.topicToTableMap = topicToTableMap;
        this.flushTimeoutMs = flushTimeoutMs;
        this.topicTableMap = topicTableMap;
        this.behaviorOnException = behaviorOnException;
        this.sessionManager = new SessionManager(client);
        this.bulkProcessor  = new BulkProcessor(
                new SystemTime(),
                new BulkKuduClient(new KuduOperate(), sessionManager),
                maxBufferedRecords,
                batchSize,
                lingerMs,
                maxRetries,
                retryBackoffMs,
                behaviorOnException,
                topicTableMap, client);
    }

    /**
     * Description 启动守护线程
     * @param
     * @return void
     * @Author dengbp
     * @Date 15:59 2020-05-22
     **/
    public void start() {
        bulkProcessor.start();
    }

    public void write(Collection<SinkRecord> records) {
        for (SinkRecord record : records){
            bulkProcessor.add(record,flushTimeoutMs);
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
        private String topicToTable;
        private long flushTimeoutMs;
        private int maxBufferedRecords;
        private int batchSize;
        private long lingerMs;
        private int maxRetry;
        private long retryBackoffMs;
        private BulkProcessor.BehaviorOnException behaviorOnException;
        private final KuduClient client;

        public Builder(KuduClient client) {
            this.client = client;
        }



        public Builder setTopicToTable(String topicToTable) {
            this.topicToTable = topicToTable;
            return this;
        }

        public Builder setFlushTimeoutMs(long flushTimeoutMs) {
            this.flushTimeoutMs = flushTimeoutMs;
            return this;
        }

        public Builder setMaxBufferedRecords(int maxBufferedRecords) {
            this.maxBufferedRecords = maxBufferedRecords;
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


        public Builder setBehaviorOnException(BulkProcessor.BehaviorOnException behaviorOnException) {
            this.behaviorOnException = behaviorOnException;
            return this;
        }




        public KuduWriter build() {
            String[] tableMaps = topicToTable.split(",");
            Map<String,String> topicToTableMap = new HashMap();
            for (String table : tableMaps){
                topicToTableMap.put(table.split(":")[0],table.split(":")[1]);
            }
            return new KuduWriter(client, topicToTableMap,flushTimeoutMs,
                    maxBufferedRecords,
                    batchSize,
                    lingerMs,
                    maxRetry,
                    retryBackoffMs,
                    topicToTableMap, behaviorOnException);
        }
    }
}
