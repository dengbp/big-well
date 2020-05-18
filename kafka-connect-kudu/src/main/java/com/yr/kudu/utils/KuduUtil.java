package com.yr.kudu.utils;

import com.alibaba.fastjson.JSONObject;
import com.yr.kudu.constant.SessionPool;
import org.apache.kudu.client.PartialRow;

import java.util.Map;

/**
 * @author baiyang
 * @version 1.0
 * @date 2020/5/15 4:34 下午
 */
public class KuduUtil {

    public static void init(String tableName) throws Exception {
        ConstantInitializationUtil.initialization(tableName);
        SessionPool.initSessionPool();
    }

    public static void typeConversion(JSONObject json, PartialRow row, String key, String type) {
        type = type.trim().toLowerCase();
        switch (type){
            case "int8":
                row.addByte(key,json.getByteValue(key));
                break;
            case "int16":
                row.addShort(key,json.getShortValue(key));
                break;
            case "int32":
                row.addInt(key,json.getInteger(key));
                break;
            case "int64":
                row.addLong(key,json.getLongValue(key));
                break;
            case "string":
                row.addString(key,json.getString(key));
                break;
            case "decimal":
                row.addDecimal(key,json.getBigDecimal(key));
                break;
            case "bool":
                row.addBoolean(key,json.getBooleanValue(key));
                break;
            case "double":
                row.addDouble(key,json.getDoubleValue(key));
                break;
            case "float":
                row.addFloat(key,json.getFloatValue(key));
                break;
            default:
                row.isNull(key);
                return;
        }
    }
}
