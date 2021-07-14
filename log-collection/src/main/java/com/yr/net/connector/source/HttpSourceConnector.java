package com.yr.net.connector.source;
import com.yr.net.collect.CollectManager;
import com.yr.net.collect.CollectType;
import com.yr.net.collect.HttpCollect;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

/**
 * @author dengbp
 * @ClassName HttpSourceConnector
 * @Description TODO
 * @date 2020-11-19 17:04
 */
@Slf4j
public class HttpSourceConnector extends SourceConnector {

    @Override
    public void start(Map<String, String> map) {
        log.info("开始初始化HttpSourceConnector...");
        long begin = System.currentTimeMillis();
        try {
            if (CollectManager.register(CollectType.HTTP_COLLECT)){
                CollectManager.getCollector(CollectType.HTTP_COLLECT).collect();
            }
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        log.info("初始化HttpSourceConnector结束...,耗时[{}]毫秒",(System.currentTimeMillis() - begin));
    }

    private void startCollectTask() throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        CollectManager.register(CollectType.HTTP_COLLECT);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return HttpSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        return null;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return null;
    }

    @Override
    public String version() {
        return null;
    }
}
