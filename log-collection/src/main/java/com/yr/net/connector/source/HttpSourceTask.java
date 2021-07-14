package com.yr.net.connector.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.List;
import java.util.Map;

/**
 * @author dengbp
 * @ClassName HttpSourceTask
 * @Description TODO
 * @date 2020-11-19 17:42
 */
@Slf4j
public class HttpSourceTask extends SourceTask {

    @Override
    public String version() {
        return null;
    }

    @Override
    public void start(Map<String, String> map) {

    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        return null;
    }

    @Override
    public void stop() {

    }
}
