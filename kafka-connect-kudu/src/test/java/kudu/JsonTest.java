package kudu;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
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
        String str = "{\n" +
                "  \"before\": {\n" +
                "    \"acct_item_id\": 49574759,\n" +
                "    \"House_id\": 4286451,\n" +
                "    \"Acct_item_type_id\": 3031,\n" +
                "    \"FEE\": \"500000.00\",\n" +
                "    \"amount\": \"2.0000\",\n" +
                "    \"unit_type\": \"\",\n" +
                "    \"Billing_cycle\": 201708,\n" +
                "    \"State\": \"POK\",\n" +
                "    \"pay_serial_id\": \"LOD00201804261542090026920000000\",\n" +
                "    \"Create_date\": 1524757329000,\n" +
                "    \"State_date\": 1524757329000,\n" +
                "    \"syn_time\": \"2020-05-21T05:22:55Z\",\n" +
                "    \"bill_date_start\": \"20170821000000\",\n" +
                "    \"bill_date_end\": \"20170821235959\",\n" +
                "    \"lfree\": \"0.00\",\n" +
                "    \"COMMUNITY_ID\": 11151132,\n" +
                "    \"lock_time\": null,\n" +
                "    \"istrans\": 0,\n" +
                "    \"channel\": 0,\n" +
                "    \"USER_ID\": null,\n" +
                "    \"task_id\": 1120246,\n" +
                "    \"pay_limit_id\": 0,\n" +
                "    \"pay_userid\": 5259711,\n" +
                "    \"bill_Flag\": 8,\n" +
                "    \"invoice_print_status\": 0,\n" +
                "    \"notice_flag\": 0,\n" +
                "    \"task_type\": 9,\n" +
                "    \"tax_rate\": \"0.00\",\n" +
                "    \"tax_fee\": \"0.0000\",\n" +
                "    \"lfree_tax_rate\": \"0.00\",\n" +
                "    \"lfree_tax_fee\": \"0.0000\",\n" +
                "    \"fee_item_type_id\": 340079,\n" +
                "    \"paid_lfree\": 0,\n" +
                "    \"paid_fee\": 500000,\n" +
                "    \"business_type\": 330145,\n" +
                "    \"owe_sync\": 1,\n" +
                "    \"lfree_begin_date\": \"\",\n" +
                "    \"lfree_rate_id\": 0,\n" +
                "    \"init_val\": \"-1.00\",\n" +
                "    \"end_val\": \"-1.00\",\n" +
                "    \"src_acct_item_id\": -1,\n" +
                "    \"adjust_count\": 0,\n" +
                "    \"obj_id\": 1120246,\n" +
                "    \"rule_inst_id\": 0,\n" +
                "    \"obj_type\": 9,\n" +
                "    \"chargeoff_fee\": 0,\n" +
                "    \"bill_rule_id\": 0,\n" +
                "    \"res_inst_id\": 4286451,\n" +
                "    \"bill_obj_type\": 1,\n" +
                "    \"data_batch\": -1,\n" +
                "    \"rate_str\": \"\",\n" +
                "    \"real_cycle\": 201708,\n" +
                "    \"real_community_id\": 11141136,\n" +
                "    \"outer_bill_id\": -1,\n" +
                "    \"account_cycle\": 201804,\n" +
                "    \"build_id\": 40033977,\n" +
                "    \"unit_id\": 40033978,\n" +
                "    \"build_name\": \"其他\",\n" +
                "    \"unit_name\": \"/\",\n" +
                "    \"house_name\": \"其他_/_场地租赁\",\n" +
                "    \"cust_name\": null,\n" +
                "    \"fee_item_type_name\": \"场地租赁保证金\",\n" +
                "    \"res_inst_code\": null,\n" +
                "    \"res_inst_name\": \"其他_/_场地租赁\",\n" +
                "    \"obj_code\": null,\n" +
                "    \"obj_name\": null,\n" +
                "    \"receivable_date\": null,\n" +
                "    \"bill_rule_name\": null,\n" +
                "    \"house_status\": null,\n" +
                "    \"house_status_type\": null,\n" +
                "    \"acct_house_code\": \"\",\n" +
                "    \"bill_area\": \"1.0000\",\n" +
                "    \"lfree_hangup_flag\": 0,\n" +
                "    \"bill_contract_id\": -1,\n" +
                "    \"first_flag\": 0,\n" +
                "    \"cust_main_id\": 0,\n" +
                "    \"lease_position\": null,\n" +
                "    \"stage_id\": 0,\n" +
                "    \"stage_name\": null,\n" +
                "    \"lfee_rate\": \"0.0000\",\n" +
                "    \"recv_base_date\": 0,\n" +
                "    \"recv_recalc_flag\": 0,\n" +
                "    \"RATE\": null,\n" +
                "    \"LOSS_RATE\": null,\n" +
                "    \"LOSS_RATE_VALUE\": null,\n" +
                "    \"diff_value\": 0.0,\n" +
                "    \"pay_rule_id\": -1,\n" +
                "    \"sys_id\": 0,\n" +
                "    \"unit_str\": null,\n" +
                "    \"belong_res_type\": 1,\n" +
                "    \"belong_res_id\": 4286451,\n" +
                "    \"belong_res_code\": null,\n" +
                "    \"belong_res_name\": \"其他_/_场地租赁\"\n" +
                "  },\n" +
                "  \"after\": {\n" +
                "    \"acct_item_id\": 49574759,\n" +
                "    \"House_id\": 4286451,\n" +
                "    \"Acct_item_type_id\": 3031,\n" +
                "    \"FEE\": \"500000.00\",\n" +
                "    \"amount\": \"1.0000\",\n" +
                "    \"unit_type\": \"\",\n" +
                "    \"Billing_cycle\": 201708,\n" +
                "    \"State\": \"POK\",\n" +
                "    \"pay_serial_id\": \"LOD00201804261542090026920000000\",\n" +
                "    \"Create_date\": 1524757329000,\n" +
                "    \"State_date\": 1524757329000,\n" +
                "    \"syn_time\": \"2020-05-21T05:25:55Z\",\n" +
                "    \"bill_date_start\": \"20170821000000\",\n" +
                "    \"bill_date_end\": \"20170821235959\",\n" +
                "    \"lfree\": \"0.00\",\n" +
                "    \"COMMUNITY_ID\": 11151132,\n" +
                "    \"lock_time\": null,\n" +
                "    \"istrans\": 0,\n" +
                "    \"channel\": 0,\n" +
                "    \"USER_ID\": null,\n" +
                "    \"task_id\": 1120246,\n" +
                "    \"pay_limit_id\": 0,\n" +
                "    \"pay_userid\": 5259711,\n" +
                "    \"bill_Flag\": 8,\n" +
                "    \"invoice_print_status\": 0,\n" +
                "    \"notice_flag\": 0,\n" +
                "    \"task_type\": 9,\n" +
                "    \"tax_rate\": \"0.00\",\n" +
                "    \"tax_fee\": \"0.0000\",\n" +
                "    \"lfree_tax_rate\": \"0.00\",\n" +
                "    \"lfree_tax_fee\": \"0.0000\",\n" +
                "    \"fee_item_type_id\": 340079,\n" +
                "    \"paid_lfree\": 0,\n" +
                "    \"paid_fee\": 500000,\n" +
                "    \"business_type\": 330145,\n" +
                "    \"owe_sync\": 1,\n" +
                "    \"lfree_begin_date\": \"\",\n" +
                "    \"lfree_rate_id\": 0,\n" +
                "    \"init_val\": \"-1.00\",\n" +
                "    \"end_val\": \"-1.00\",\n" +
                "    \"src_acct_item_id\": -1,\n" +
                "    \"adjust_count\": 0,\n" +
                "    \"obj_id\": 1120246,\n" +
                "    \"rule_inst_id\": 0,\n" +
                "    \"obj_type\": 9,\n" +
                "    \"chargeoff_fee\": 0,\n" +
                "    \"bill_rule_id\": 0,\n" +
                "    \"res_inst_id\": 4286451,\n" +
                "    \"bill_obj_type\": 1,\n" +
                "    \"data_batch\": -1,\n" +
                "    \"rate_str\": \"\",\n" +
                "    \"real_cycle\": 201708,\n" +
                "    \"real_community_id\": 11141136,\n" +
                "    \"outer_bill_id\": -1,\n" +
                "    \"account_cycle\": 201804,\n" +
                "    \"build_id\": 40033977,\n" +
                "    \"unit_id\": 40033978,\n" +
                "    \"build_name\": \"其他\",\n" +
                "    \"unit_name\": \"/\",\n" +
                "    \"house_name\": \"其他_/_场地租赁\",\n" +
                "    \"cust_name\": null,\n" +
                "    \"fee_item_type_name\": \"场地租赁保证金\",\n" +
                "    \"res_inst_code\": null,\n" +
                "    \"res_inst_name\": \"其他_/_场地租赁\",\n" +
                "    \"obj_code\": null,\n" +
                "    \"obj_name\": null,\n" +
                "    \"receivable_date\": null,\n" +
                "    \"bill_rule_name\": null,\n" +
                "    \"house_status\": null,\n" +
                "    \"house_status_type\": null,\n" +
                "    \"acct_house_code\": \"\",\n" +
                "    \"bill_area\": \"1.0000\",\n" +
                "    \"lfree_hangup_flag\": 0,\n" +
                "    \"bill_contract_id\": -1,\n" +
                "    \"first_flag\": 0,\n" +
                "    \"cust_main_id\": 0,\n" +
                "    \"lease_position\": null,\n" +
                "    \"stage_id\": 0,\n" +
                "    \"stage_name\": null,\n" +
                "    \"lfee_rate\": \"0.0000\",\n" +
                "    \"recv_base_date\": 0,\n" +
                "    \"recv_recalc_flag\": 0,\n" +
                "    \"RATE\": null,\n" +
                "    \"LOSS_RATE\": null,\n" +
                "    \"LOSS_RATE_VALUE\": null,\n" +
                "    \"diff_value\": 0.0,\n" +
                "    \"pay_rule_id\": -1,\n" +
                "    \"sys_id\": 0,\n" +
                "    \"unit_str\": null,\n" +
                "    \"belong_res_type\": 1,\n" +
                "    \"belong_res_id\": 4286451,\n" +
                "    \"belong_res_code\": null,\n" +
                "    \"belong_res_name\": \"其他_/_场地租赁\"\n" +
                "  },\n" +
                "  \"source\": {\n" +
                "    \"version\": \"0.9.4.Final\",\n" +
                "    \"connector\": \"mysql\",\n" +
                "    \"name\": \"dev_12_uhome\",\n" +
                "    \"server_id\": 1,\n" +
                "    \"ts_sec\": 1590038755,\n" +
                "    \"gtid\": null,\n" +
                "    \"file\": \"mysql-bin.000123\",\n" +
                "    \"pos\": 15028745,\n" +
                "    \"row\": 0,\n" +
                "    \"snapshot\": false,\n" +
                "    \"thread\": 3341343,\n" +
                "    \"db\": \"segi_dmp\",\n" +
                "    \"table\": \"tb_uhome_acct_item_tmp_1\",\n" +
                "    \"query\": null\n" +
                "  },\n" +
                "  \"op\": \"c\",\n" +
                "  \"ts_ms\": 1590039353486\n" +
                "}";

        final String KUDU_MASTERS = System.getProperty("kuduMasters", "192.168.1.9:7051");
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();
        KuduSession session = client.newSession();
        KuduUtil.init(client,"tb_uhome_acct_item");

        BingLog bingLog = JSON.parseObject(str, BingLog.class);
//        log.info("request.getValues()={}",str.getValues());
        CaseInsensitiveMap mysqlSource;
        Operation operation;
        String tableName = "tb_uhome_acct_item";
        KuduTable kuduTable = client.openTable("tb_uhome_acct_item");
        if(BingLog.DELETE.equals(bingLog.getOp())){
            mysqlSource = bingLog.getBefore();
            operation = kuduTable.newDelete();
        } else if(BingLog.UPDATE.equals(bingLog.getOp())) {
            mysqlSource = bingLog.getAfter();
            operation = kuduTable.newUpdate();
        } else {
            mysqlSource = bingLog.getAfter();
            operation = kuduTable.newInsert();
        }
        PartialRow row = operation.getRow();
        Map<String,String> kuduTableType = TableTypeConstantMap.kuduTables.get(tableName);
        Object[] objects = kuduTableType.keySet().toArray();
        for(int i = 0; i < objects.length; i++){
            String key = objects[i].toString();
            String type = kuduTableType.get(key);
            KuduUtil.typeConversion(mysqlSource, row, key, type);
        }
        session.apply(operation);
        System.out.println();
//        judge(session);
    }
}
