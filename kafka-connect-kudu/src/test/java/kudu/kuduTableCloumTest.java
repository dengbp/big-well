package kudu;

import com.yr.kudu.session.TableTypeConstantMap;
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
//        KuduUtil.init("tb_uhome_acct_item,tb_uhome_house,user");
        Map<String, Map<String, String>> kuduTables = TableTypeConstantMap.kuduTables;
//        BlockingQueue<KuduSession> sessions = SessionManager.getSessions();

    }

    @Test
    public void myTest2() throws Exception {
        String str = "10000000000.000011";
        Double l = Double.parseDouble(str);
        BigDecimal b = BigDecimal.valueOf(valueOf(l));
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
