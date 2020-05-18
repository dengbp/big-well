package kudu;


import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.connect.json.JsonConverter;

import java.nio.charset.StandardCharsets;
import java.util.Map;


/**
 * @author dengbp
 * @ClassName JsonTest
 * @Description TODO
 * @date 2020-05-18 15:36
 */
public class JsonTest {

    public static void main(String[] args) {
String str = "{fee_item_type_name=场地租赁保证金, belong_res_name=其他_/_场地租赁, bill_rule_id=0, lfree_tax_fee=AA==, State_date=1524757329000, pay_userid=5259711, end_val=nA==, Create_date=1524757329000, channel=0, USER_ID=null, paid_fee=500000, res_inst_name=其他_/_场地租赁, LOSS_RATE=null, fee_item_type_id=340079, tax_rate=AA==, lfree_tax_rate=AA==, account_cycle=201804, bill_Flag=8, outer_bill_id=-1, stage_name=null, obj_type=9, LOSS_RATE_VALUE=null, pay_serial_id=LOD00201804261542090026920000000, obj_code=null, cust_main_id=0, stage_id=0, bill_area=JxA=, bill_rule_name=null, rule_inst_id=0, lease_position=null, unit_name=/, pay_limit_id=0, invoice_print_status=0, notice_flag=0, belong_res_type=1, build_id=40033977, State=POK, bill_contract_id=-1, chargeoff_fee=0, paid_lfree=0, pay_rule_id=-1, bill_date_end=20170821235959, bill_obj_type=1, recv_base_date=0, obj_name=null, house_status=null, RATE=null, acct_item_id=49574759, diff_value=0.0, task_id=1120246, belong_res_id=4286451, unit_type=, adjust_count=0, build_name=其他, lfee_rate=AA==, sys_id=0, COMMUNITY_ID=11151132, tax_fee=AA==, acct_house_code=, house_name=其他_/_场地租赁, receivable_date=null, business_type=330145, res_inst_id=4286451, Billing_cycle=201708, real_community_id=11141136, cust_name=null, recv_recalc_flag=0, unit_id=40033978, res_inst_code=null, amount=JxA=, rate_str=, house_status_type=null, obj_id=1120246, unit_str=null, FEE=AvrwgA==, lfree_begin_date=, House_id=4286451, belong_res_code=null, src_acct_item_id=-1, real_cycle=201708, lfree_hangup_flag=0, owe_sync=1, lock_time=null, Acct_item_type_id=3031, init_val=nA==, data_batch=-1, bill_date_start=20170821000000, lfree=AA==, syn_time=2020-04-11T08:16:06Z, task_type=9, istrans=0, lfree_rate_id=0, first_flag=0}";
        org.apache.kafka.connect.json.JsonConverter jsonConverter = new JsonConverter();





JSONObject.parseObject(str, Map.class);
        System.out.println();
    }
}
