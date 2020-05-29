package com.yr.kudu.utils;

import com.yr.kudu.session.TableTypeConstantMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduTable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author baiyang
 * @version 1.0
 * @date 2020/5/16 10:14 上午
 */
@Slf4j
public class ConstantInitializationUtil {

    /**
     * Description todo
     * @param client
 * @param tableName	 tb_uhome_acct_item:tb_uhome_acct_item,tb_uhome_house:tb_uhome_house
     * @return void
     * @Author baiYang
     * @Date 15:37 2020-05-29
     **/

    public static void initialization(KuduClient client, String tableName) throws Exception {
        String[] tableNames = tableName.split(",");
        log.info("init kudu table size={},tableNames={}",tableNames.length,tableName);
        if(tableNames.length ==0){
            return;
        }
        for(String map : tableNames){
            String[] ss = map.split(":");
            TableTypeConstantMap.sourceSinkMap.put(ss[0],ss[1]);
            KuduTable kuduTable = client.openTable(ss[1]);
            List<ColumnSchema> columns = kuduTable.getSchema().getColumns();
            Map<String, String> stringMapHashMap = new HashMap<>(columns.size());
            List<String> primaryKeys = new ArrayList<>();
            for (ColumnSchema c : columns){
                if(c.isKey()){
                    primaryKeys.add(c.getName());
                }
                stringMapHashMap.put(c.getName(), c.getType().getName());
            }
            TableTypeConstantMap.kuduTables.put(ss[1],stringMapHashMap);
            TableTypeConstantMap.kuduPrimaryKey.put(ss[1],primaryKeys);
        }
    }
}
