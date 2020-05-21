package kudu;

import com.yr.connector.bulk.KuduOperate;
import com.yr.kudu.session.TableTypeConstantMap;
import com.yr.kudu.utils.DateUtil;
import com.yr.kudu.utils.KuduUtil;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.kudu.client.*;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import static java.lang.Double.valueOf;

/**
 * @author baiyang
 * @version 1.0
 * @date 2020/5/18 3:50 下午
 */
public class kuduTableCloumTest {
    @Test
    public void myTest() throws Exception {
       final String KUDU_MASTERS = System.getProperty("kuduMasters", "192.168.1.9:7051");
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();
        KuduSession session = client.newSession();
        KuduUtil.init(client,"tb_uhome_acct_item");
        Map<String, Map<String, String>> kuduTables = TableTypeConstantMap.kuduTables;

        KuduTable tb_uhome_acct_item = client.openTable("tb_uhome_acct_item");
        Insert insert = tb_uhome_acct_item.newInsert();
        PartialRow row = insert.getRow();
        Object[] objects = kuduTables.get("tb_uhome_acct_item").keySet().toArray();
        for(int i = 0; i < objects.length; i++){
            String key = objects[i].toString();
            String type = kuduTables.get("tb_uhome_acct_item").get(key);
            KuduUtil.typeConversion(new CaseInsensitiveMap(), row, key, type);
        }
        System.out.println(kuduTables.get("tb_uhome_acct_item").get("create_date"));

    }

    @Test
    public void myTest2() throws Exception {
        String str = "10000000000.000011";
        Double l = Double.parseDouble(str);
        BigDecimal b = BigDecimal.valueOf(valueOf(l));
        String s = DateUtil.millisecondFormat(1540489316000L, "yyyy-MM-dd HH:mm:ss");
        System.out.println();
    }

    public static Map<String,String> mapStringToMap(String str){
        str=str.substring(1, str.length()-1);
        String[] strs=str.split(",");
        Map<String,String> map = new HashMap<String, String>();
        for (String string : strs) {
            String key=string.split("=")[0];
            String value=string.split("=")[1];
            map.put(key, value);
        }
        return map;
    }
}
