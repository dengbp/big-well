package com.yr.kudu.session;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 需要同步表的信息
 * Map<表名,Map<列名,类型>>
 * @author baiyang
 * @version 1.0
 * @date 2020/5/15 6:26 下午
 */
public class TableTypeConstantMap {
    private TableTypeConstantMap(){}

    private static TableTypeConstantMap  tableTypeConstantMap;
    /**
     * 存储需要同步的表信息 Map<表名，map<列名，字段类型>>
     */
    public static Map<String,Map<String,String>> kuduTables = new HashMap<>();

    /**
     * 存储每个表的主键信息 Map<表名，List<主键列名>>
     */
    public static Map<String, List<String>> kuduPrimaryKey =  new HashMap<>();

    public static synchronized TableTypeConstantMap getTableTypeConstantMap(){
        if(null == tableTypeConstantMap){
          return new TableTypeConstantMap();
        } else {
            return tableTypeConstantMap;
        }

    }

}
