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
     *  需要同步表的信息初始化（包含Map<表名，Map<字段名，类型名称>>）
     * @throws Exception
     */
    public static void initialization(KuduClient client, String tableName) throws Exception {
        String[] tableNames = tableName.split(",");
        log.info("init kudu table size={},tableNames={}",tableNames.length,tableName);
        if(tableNames.length ==0){
            return;
        }
        for(String s : tableNames){
            KuduTable kuduTable = client.openTable(s);
            List<ColumnSchema> columns = kuduTable.getSchema().getColumns();
            Map<String, String> stringMapHashMap = new HashMap<>(columns.size());
            List<String> primaryKeys = new ArrayList<>();
            for (ColumnSchema c : columns){
                if(c.isKey()){
                    primaryKeys.add(c.getName());
                }
                stringMapHashMap.put(c.getName(), c.getType().getName());
            }
            TableTypeConstantMap.kuduTables.put(s,stringMapHashMap);
            TableTypeConstantMap.kuduPrimaryKey.put(s,primaryKeys);
        }
    }
}
