package com.yr.connector;

import com.alibaba.fastjson.JSONObject;
import com.yr.kudu.utils.ConstantInitializationUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author dengbpcom.yr.connector
 * @ClassName TableStreamSinkConnector
 * @Description TODO
 * @date 2020-05-15 18:19
 */
@Slf4j
public class KuduSinkConnector extends SinkConnector {

    private Map<String, String> configProperties;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("KuduSinkConnector begin start...");
        configProperties = props;
        log.info("receive config:{}", JSONObject.toJSONString(configProperties));
        new KuduSinkConnectorConfig(props);
        try {
            ConstantInitializationUtil.initialization("all");
        } catch (Exception e) {
            e.printStackTrace();
            log.error("初始化失败...");
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return KuduSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("KuduTaskConfigs begin setting...");
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        Map<String, String> taskProps = new HashMap<>();
        taskProps.putAll(configProperties);
        for (int i = 0; i < maxTasks; i++) {
            configs.add(taskProps);
        }
        return configs;
    }

    @Override
    public void stop() {
        // Nothing to do since TableStreamSinkConnector has no background monitoring.
        log.info("KuduSinkConnector begin stop...");
    }

    @Override
    public ConfigDef config() {
        return KuduSinkConnectorConfig.CONFIG;
    }
}
