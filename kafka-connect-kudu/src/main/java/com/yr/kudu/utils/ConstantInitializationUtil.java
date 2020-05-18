package com.yr.kudu.utils;

import com.yr.kudu.constant.SessionPool;
import com.yr.kudu.constant.TableTypeConstantMap;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author baiyang
 * @version 1.0
 * @date 2020/5/16 10:14 上午
 */
public class ConstantInitializationUtil {
    /**
     *  需要同步表的信息初始化（包含Map<表名，Map<字段名，类型名称>>）
     * @throws Exception
     */
    public static void initialization(String tableName) throws Exception {
        String[] tableNames = tableName.split(",");
        if(tableNames.length ==0){
            return;
        }
        for(String s : tableNames){
            KuduTable kuduTable = SessionPool.client.openTable(s);
            List<ColumnSchema> columns = kuduTable.getSchema().getColumns();
            Map<String, String> stringMapHashMap = new HashMap<>(columns.size());
            for (ColumnSchema c : columns){
                stringMapHashMap.put(c.getName(), c.getType().getName());
            }
            TableTypeConstantMap.kuduTables.put(s,stringMapHashMap);
        }
    }
}
