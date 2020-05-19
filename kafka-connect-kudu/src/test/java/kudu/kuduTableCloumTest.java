//package kudu;
//
//import com.yr.kudu.session.SessionManager;
//import com.yr.kudu.session.TableTypeConstantMap;
//import com.yr.connector.bulk.KuduOperate;
//import com.yr.kudu.utils.KuduUtil;
//import com.yr.pojo.TableValues;
//import org.apache.kudu.client.KuduSession;
//import org.junit.Test;
//
//import java.util.HashMap;
//import java.util.Map;
//import java.util.concurrent.BlockingQueue;
//
///**
// * @author baiyang
// * @version 1.0
// * @date 2020/5/18 3:50 下午
// */
//public class kuduTableCloumTest {
//    @Test
//    public void myTest() throws Exception {
//        KuduUtil.init("tb_uhome_acct_item,tb_uhome_house,user");
//        Map<String, Map<String, String>> kuduTables = TableTypeConstantMap.kuduTables;
//        BlockingQueue<KuduSession> sessions = SessionManager.getSessions();
//        System.out.println(kuduTables.size());
//        System.out.println(sessions.size());
//    }
//
//    @Test
//    public void myTest2() throws Exception {
//        TableValues tb_uhome_acct_item = new TableValues("tb_uhome_acct_item", "{\"acct_item_id\":49574759,\"House_id\":4286451,\"Acct_item_type_id\":3031,\"FEE\":\"AvrwgA==\",\"amount\":\"JxA=\",\"unit_type\":\"\",\"Billing_cycle\":201708,\"State\":\"POK\",\"pay_serial_id\":\"LOD00201804261542090026920000000\",\"Create_date\":1524757329000,\"State_date\":1524757329000,\"syn_time\":\"2020-04-10T19:16:06Z\",\"bill_date_start\":\"20170821000000\",\"bill_date_end\":\"20170821235959\",\"lfree\":\"AA==\",\"COMMUNITY_ID\":11151132,\"lock_time\":null,\"istrans\":0,\"channel\":0,\"USER_ID\":null,\"task_id\":1120246,\"pay_limit_id\":0,\"pay_userid\":5259711,\"bill_Flag\":8,\"invoice_print_status\":0,\"notice_flag\":0,\"task_type\":9,\"tax_rate\":\"AA==\",\"tax_fee\":\"AA==\",\"lfree_tax_rate\":\"AA==\",\"lfree_tax_fee\":\"AA==\",\"fee_item_type_id\":340079,\"paid_lfree\":0,\"paid_fee\":500000,\"business_type\":330145,\"owe_sync\":1,\"lfree_begin_date\":\"\",\"lfree_rate_id\":0,\"init_val\":\"nA==\",\"end_val\":\"nA==\",\"src_acct_item_id\":-1,\"adjust_count\":0,\"obj_id\":1120246,\"rule_inst_id\":0,\"obj_type\":9,\"chargeoff_fee\":0,\"bill_rule_id\":0,\"res_inst_id\":4286451,\"bill_obj_type\":1,\"data_batch\":-1,\"rate_str\":\"\",\"real_cycle\":201708,\"real_community_id\":11141136,\"outer_bill_id\":-1,\"account_cycle\":201804,\"build_id\":40033977,\"unit_id\":40033978,\"build_name\":\"其他\",\"unit_name\":\"/\",\"house_name\":\"其他_/_场地租赁\",\"cust_name\":null,\"fee_item_type_name\":\"场地租赁保证金\",\"res_inst_code\":null,\"res_inst_name\":\"其他_/_场地租赁\",\"obj_code\":null,\"obj_name\":null,\"receivable_date\":null,\"bill_rule_name\":null,\"house_status\":null,\"house_status_type\":null,\"acct_house_code\":\"\",\"bill_area\":\"JxA=\",\"lfree_hangup_flag\":0,\"bill_contract_id\":-1,\"first_flag\":0,\"cust_main_id\":0,\"lease_position\":null,\"stage_id\":0,\"stage_name\":null,\"lfee_rate\":\"AA==\",\"recv_base_date\":0,\"recv_recalc_flag\":0,\"RATE\":null,\"LOSS_RATE\":null,\"LOSS_RATE_VALUE\":null,\"diff_value\":0.0,\"pay_rule_id\":-1,\"sys_id\":0,\"unit_str\":null,\"belong_res_type\":1,\"belong_res_id\":4286451,\"belong_res_code\":null,\"belong_res_name\":\"其他_/_场地租赁\"}");
//        KuduUtil.init("tb_uhome_acct_item,tb_uhome_house,user");
//        KuduOperate.insert(tb_uhome_acct_item);
//
//    }
//
//    public static Map<String,String> mapStringToMap(String str){
//        str=str.substring(1, str.length()-1);
//        String[] strs=str.split(",");
//        Map<String,String> map = new HashMap<String, String>();
//        for (String string : strs) {
//            String key=string.split("=")[0];
//            String value=string.split("=")[1];
//            map.put(key, value);
//        }
//        return map;
//    }
//}
