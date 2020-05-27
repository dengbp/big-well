package kudu;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yr.connector.bulk.BulkRequest;
import com.yr.connector.bulk.KuduOperate;
import com.yr.kudu.pojo.BingLog;
import com.yr.kudu.session.TableTypeConstantMap;
import com.yr.kudu.utils.KuduUtil;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kudu.client.*;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;



/**
 * @author dengbp
 * @ClassName JsonTest
 * @Description TODO
 * @date 2020-05-18 15:36
 */
public class JsonTest {

    public static void main(String[] args) throws Exception {
        String str = "{\"before\":null,\"after\":{\"id\":3,\"username\":\"rentApp\",\"password\":\"abcdefg123\",\"url\":\"jdbc:mysql://192.168.1.12:3306/segi_dmp?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=GMT%2b8\",\"type\":1,\"creat_time\":20200511150855,\"created_by\":null,\"updated_by\":\"\",\"updated_dt\":null,\"status\":1,\"table_schema\":\"segi_dmp\"},\"source\":{\"version\":\"0.9.4.Final\",\"connector\":\"mysql\",\"name\":\"segi_dmp_connector\",\"server_id\":0,\"ts_sec\":0,\"gtid\":null,\"file\":\"mysql-bin.000124\",\"pos\":121202188,\"row\":0,\"snapshot\":true,\"thread\":null,\"db\":\"segi_dmp\",\"table\":\"dmp_datasource_info\",\"query\":null},\"op\":\"c\",\"ts_ms\":1590490572518}";

        final String KUDU_MASTERS = System.getProperty("kuduMasters", "slave1:7051");
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();
        KuduSession session = client.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        session.setMutationBufferSpace(10000);
        KuduUtil.init(client,"dmp_datasource_info");


//        log.info("request.getValues()={}",str.getValues());
        String tableName = "dmp_datasource_info";
        KuduTable kuduTable = client.openTable("dmp_datasource_info");
        BulkRequest bulkRequest = new BulkRequest(kuduTable, tableName, str);
        new KuduOperate().operation(session,bulkRequest);
        session.flush();
        session.close();
    }
}
