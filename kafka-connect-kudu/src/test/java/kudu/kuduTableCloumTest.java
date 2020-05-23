package kudu;

import com.alibaba.fastjson.JSON;
import com.yr.kudu.session.TableTypeConstantMap;
import com.yr.kudu.utils.DateUtil;
import com.yr.kudu.utils.KuduUtil;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.kudu.client.*;
import org.junit.Test;

import java.math.BigDecimal;
import java.text.ParseException;
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
        Delete delete = tb_uhome_acct_item.newDelete();
        PartialRow row1 = delete.getRow();
        PartialRow row = insert.getRow();
        Object[] objects = kuduTables.get("tb_uhome_acct_item").keySet().toArray();
        for(int i = 0; i < objects.length; i++){
            String key = objects[i].toString();
            String type = kuduTables.get("tb_uhome_acct_item").get(key);
            KuduUtil.typeConversion(new CaseInsensitiveMap(), row1, key, type);
        }

        System.out.println(kuduTables.get("tb_uhome_acct_item").get("create_date"));

    }

    public static void test22(KuduSession session,KuduClient client) throws KuduException, ParseException {
        String str = "{\"acct_item_id\":70902614,\"House_id\":3348138,\"Acct_item_type_id\":3025,\"FEE\":\"800.00\",\"amount\":\"1.0000\",\"unit_type\":\"\",\"Billing_cycle\":201812,\"State\":\"POK\",\"pay_serial_id\":\"AOF00201901191005350007861000000\",\"Create_date\":1540489316000,\"State_date\":1547892335000,\"syn_time\":\"2020-04-10T19:58:17Z\",\"bill_date_start\":\"20181201000000\",\"bill_date_end\":\"20181231235959\",\"lfree\":\"0.00\",\"COMMUNITY_ID\":11203010,\"lock_time\":null,\"istrans\":0,\"channel\":2,\"USER_ID\":null,\"task_id\":800862,\"pay_limit_id\":0,\"pay_userid\":4670612,\"bill_Flag\":2,\"invoice_print_status\":0,\"notice_flag\":0,\"task_type\":1,\"tax_rate\":\"0.00\",\"tax_fee\":\"0.0000\",\"lfree_tax_rate\":\"0.00\",\"lfree_tax_fee\":\"0.0000\",\"fee_item_type_id\":340025,\"paid_lfree\":0,\"paid_fee\":800,\"business_type\":700473,\"owe_sync\":1,\"lfree_begin_date\":\"\",\"lfree_rate_id\":0,\"init_val\":\"-1.00\",\"end_val\":\"-1.00\",\"src_acct_item_id\":-1,\"adjust_count\":0,\"obj_id\":800862,\"rule_inst_id\":3824407,\"obj_type\":1,\"chargeoff_fee\":0,\"bill_rule_id\":570053,\"res_inst_id\":3348138,\"bill_obj_type\":1,\"data_batch\":-1,\"rate_str\":\"8\",\"real_cycle\":201812,\"real_community_id\":11203008,\"outer_bill_id\":-1,\"account_cycle\":201810,\"build_id\":11346134,\"unit_id\":11336037,\"build_name\":\"5栋\",\"unit_name\":\"1单元\",\"house_name\":\"5栋_1单元_3402\",\"cust_name\":\"\",\"fee_item_type_name\":\"生活垃圾处理费\",\"res_inst_code\":\"\",\"res_inst_name\":\"5栋_1单元_3402\",\"obj_code\":\"\",\"obj_name\":\"\",\"receivable_date\":0,\"bill_rule_name\":\"生活垃圾处理费(住宅：8元/户/月)\",\"house_status\":\"\",\"house_status_type\":\"\",\"acct_house_code\":\"\",\"bill_area\":\"72.1400\",\"lfree_hangup_flag\":0,\"bill_contract_id\":-1,\"first_flag\":0,\"cust_main_id\":0,\"lease_position\":null,\"stage_id\":0,\"stage_name\":null,\"lfee_rate\":\"0.0000\",\"recv_base_date\":0,\"recv_recalc_flag\":0,\"RATE\":null,\"LOSS_RATE\":null,\"LOSS_RATE_VALUE\":null,\"diff_value\":0.0,\"pay_rule_id\":-1,\"sys_id\":0,\"unit_str\":null,\"belong_res_type\":1,\"belong_res_id\":3348138,\"belong_res_code\":\"\",\"belong_res_name\":\"5栋_1单元_3402\"}";
        CaseInsensitiveMap map = JSON.parseObject(str,CaseInsensitiveMap.class);
        String tableName = "tb_uhome_acct_item";
        KuduTable kuduTable = client.openTable(tableName);
//        Delete delete = kuduTable.newDelete();
//        PartialRow row = delete.getRow();
        Insert insert = kuduTable.newInsert();
        PartialRow row = insert.getRow();
        Map<String,String> kuduTableType = TableTypeConstantMap.kuduTables.get(tableName);
        Object[] objects = kuduTableType.keySet().toArray();
        for(int i = 0; i < objects.length; i++){
            String key = objects[i].toString();
            String type = kuduTableType.get(key);
            KuduUtil.typeConversion(map, row, key, type);
        }
        session.apply(insert);

    }

    @Test
    public void deleteTest() throws Exception {
        final String KUDU_MASTERS = System.getProperty("kuduMasters", "192.168.1.9:7051");
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();
        KuduSession session = client.newSession();
        KuduUtil.init(client,"tb_uhome_acct_item");
        test22(session,client);
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
    @Test
    public void myDateTest() throws ParseException {
        String str = "2020-05-20T09:09:23Z";
        str = str.replace("T", " ").replace("Z", "");
        DateUtil.getMillisecond(str, "yyyy-MM-dd HH:mm:ss");
        System.out.println();
    }

}
