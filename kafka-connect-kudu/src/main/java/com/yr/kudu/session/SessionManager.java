package com.yr.kudu.session;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.SessionConfiguration;

/**
 * @author baiyang
 * @version 1.0
 * @date 2020/5/17 3:01 下午
 */
public class SessionManager {
    public static final int OPERATION_BATCH = 10000;
    private final KuduClient client;
    private static final int TIMEOUT_MILLIS = 6000;

    public SessionManager(KuduClient client) {
        this.client = client;
    }


    /**
     * Description todo
     * @return void
     * @Author dengbp
     * @Date 16:17 2020-05-19
     **/
    public KuduSession getSession(){
        KuduSession kuduSession = client.newSession();
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        kuduSession.setMutationBufferSpace(OPERATION_BATCH);
        kuduSession.setTimeoutMillis(TIMEOUT_MILLIS);
        return kuduSession;
    }


    private static void closeSession(KuduSession kuduSession){
        if (!kuduSession.isClosed()) {
            try {
                kuduSession.close();
            } catch (KuduException e) {
                e.printStackTrace();
            }
        }
    }
}
