package com.yr.connector;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.Map;

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

    @Override
    public void start(Map<String, String> props) {
        log.info("KuduSinkTask begin start...");
        client = new KuduClient(props);
        KuduWriter.Builder builder = new KuduWriter.Builder(client);
        writer = builder.build();
        writer.start();
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        log.info("Putting {} records to Kudu", sinkRecords.size());
        writer.write(sinkRecords);
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        log.info("Flushing data to kudu with the following offsets: {}", offsets);
        writer.flush();
    }

    @Override
    public void stop() {
        log.info("Stopping kuduSinkTask");
        if (writer != null) {
            writer.stop();
        }
        if (client != null) {
            client.close();
        }
    }
}
