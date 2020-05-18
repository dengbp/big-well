package com.yr.kudu.constant;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author baiyang
 * @version 1.0
 * @date 2020/5/17 3:01 下午
 */
public class SessionPool {
    private static BlockingQueue<KuduSession> sessions = new LinkedBlockingQueue<>();
    static final int cpuNum = Runtime.getRuntime().availableProcessors() * 2;
    private static final String KUDU_MASTERS = System.getProperty("kuduMasters", "192.168.1.9:7051");
    public static KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();

    public static BlockingQueue<KuduSession> getSessions() {
        return sessions;
    }
    public static void initSessionPool(){
        sessions.clear();
        int i = 0;
        while (i < cpuNum){
            KuduSession kuduSession = client.newSession();
            sessions.add(kuduSession);
            i++;
        }
    }
}
