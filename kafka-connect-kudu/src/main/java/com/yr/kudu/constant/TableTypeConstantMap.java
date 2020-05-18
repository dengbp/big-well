package com.yr.kudu.constant;

import java.util.HashMap;
import java.util.Map;

/**
 * 需要同步表的信息
 * Map<表名,Map<列名,类型>>
 * @author baiyang
 * @version 1.0
 * @date 2020/5/15 6:26 下午
 */
public class TableTypeConstantMap {
    private TableTypeConstantMap(){};

    private static TableTypeConstantMap  tableTypeConstantMap;
    /**
     * 计费用表
     */
    public static Map<String,Map<String,String>> billTables = new HashMap<>();

    /**
     * 基础信息用表
     */
    public static Map<String,Map<String,String>> basicTables = new HashMap<>();



    public static synchronized TableTypeConstantMap getTableTypeConstantMap(){
        if(null == tableTypeConstantMap){
          return new TableTypeConstantMap();
        } else {
            return tableTypeConstantMap;
        }

    }

}