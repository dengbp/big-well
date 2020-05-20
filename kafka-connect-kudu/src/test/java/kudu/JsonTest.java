package kudu;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.kafka.connect.json.JsonConverter;

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

    public static void main(String[] args) {
String str = "{\"acct_item_id\":70902614,\"House_id\":3348138,\"Acct_item_type_id\":3025,\"FEE\":\"800.00\",\"amount\":\"1.0000\",\"unit_type\":\"\",\"Billing_cycle\":201812,\"State\":\"POK\",\"pay_serial_id\":\"AOF00201901191005350007861000000\",\"Create_date\":1540489316000,\"State_date\":1547892335000,\"syn_time\":\"2020-04-10T19:58:17Z\",\"bill_date_start\":\"20181201000000\",\"bill_date_end\":\"20181231235959\",\"lfree\":\"0.00\",\"COMMUNITY_ID\":11203010,\"lock_time\":null,\"istrans\":0,\"channel\":2,\"USER_ID\":null,\"task_id\":800862,\"pay_limit_id\":0,\"pay_userid\":4670612,\"bill_Flag\":2,\"invoice_print_status\":0,\"notice_flag\":0,\"task_type\":1,\"tax_rate\":\"0.00\",\"tax_fee\":\"0.0000\",\"lfree_tax_rate\":\"0.00\",\"lfree_tax_fee\":\"0.0000\",\"fee_item_type_id\":340025,\"paid_lfree\":0,\"paid_fee\":800,\"business_type\":700473,\"owe_sync\":1,\"lfree_begin_date\":\"\",\"lfree_rate_id\":0,\"init_val\":\"-1.00\",\"end_val\":\"-1.00\",\"src_acct_item_id\":-1,\"adjust_count\":0,\"obj_id\":800862,\"rule_inst_id\":3824407,\"obj_type\":1,\"chargeoff_fee\":0,\"bill_rule_id\":570053,\"res_inst_id\":3348138,\"bill_obj_type\":1,\"data_batch\":-1,\"rate_str\":\"8\",\"real_cycle\":201812,\"real_community_id\":11203008,\"outer_bill_id\":-1,\"account_cycle\":201810,\"build_id\":11346134,\"unit_id\":11336037,\"build_name\":\"5栋\",\"unit_name\":\"1单元\",\"house_name\":\"5栋_1单元_3402\",\"cust_name\":\"\",\"fee_item_type_name\":\"生活垃圾处理费\",\"res_inst_code\":\"\",\"res_inst_name\":\"5栋_1单元_3402\",\"obj_code\":\"\",\"obj_name\":\"\",\"receivable_date\":0,\"bill_rule_name\":\"生活垃圾处理费(住宅：8元/户/月)\",\"house_status\":\"\",\"house_status_type\":\"\",\"acct_house_code\":\"\",\"bill_area\":\"72.1400\",\"lfree_hangup_flag\":0,\"bill_contract_id\":-1,\"first_flag\":0,\"cust_main_id\":0,\"lease_position\":null,\"stage_id\":0,\"stage_name\":null,\"lfee_rate\":\"0.0000\",\"recv_base_date\":0,\"recv_recalc_flag\":0,\"RATE\":null,\"LOSS_RATE\":null,\"LOSS_RATE_VALUE\":null,\"diff_value\":0.0,\"pay_rule_id\":-1,\"sys_id\":0,\"unit_str\":null,\"belong_res_type\":1,\"belong_res_id\":3348138,\"belong_res_code\":\"\",\"belong_res_name\":\"5栋_1单元_3402\"}";
        CaseInsensitiveMap map = JSON.parseObject(str,CaseInsensitiveMap.class);

        System.out.println(map.get("acct_item_type_id"));

    }
}
