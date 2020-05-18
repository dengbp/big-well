package kudu;

import com.yr.kudu.constant.SessionPool;
import com.yr.kudu.constant.TableTypeConstantMap;
import com.yr.kudu.utils.KuduUtil;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.junit.Test;
import sun.tools.jconsole.Tab;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * @author baiyang
 * @version 1.0
 * @date 2020/5/18 3:50 下午
 */
public class kuduTableCloumTest {
    @Test
    public void myTest() throws Exception {
        KuduUtil.init("tb_uhome_acct_item,tb_uhome_house,user");
        Map<String, Map<String, String>> kuduTables = TableTypeConstantMap.kuduTables;
        BlockingQueue<KuduSession> sessions = SessionPool.getSessions();
        System.out.println(kuduTables.size());
        System.out.println(sessions.size());
    }
}
