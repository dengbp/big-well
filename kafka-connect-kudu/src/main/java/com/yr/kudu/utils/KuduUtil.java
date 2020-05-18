package com.yr.kudu.utils;

import com.alibaba.fastjson.JSONObject;
import com.yr.kudu.constant.SessionPool;
import org.apache.kudu.client.PartialRow;

import java.math.BigDecimal;
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
        key = key.trim().toLowerCase();
        switch (type){
            case "int8":
                Byte byteValue = json.getByte(key);
                if(null == byteValue){
                    row.isNull(key);
                    break;
                }
                row.addByte(key,byteValue);
                break;
            case "int16":
                Short aShort = json.getShort(key);
                if(null == aShort){
                    row.isNull(key);
                    break;
                }
                row.addShort(key,aShort);
                break;
            case "int32":
                Integer integer = json.getInteger(key);
                if(integer == null){
                    row.isNull(key);
                    break;
                }
                row.addInt(key,integer);
                break;
            case "int64":
                row.addLong(key,json.getLongValue(key));
                break;
            case "string":
                String string = json.getString(key);
                if(string == null){
                    row.isNull(key);
                    break;
                }
                row.addString(key,string);
                break;
            case "decimal":
                BigDecimal bigDecimal = json.getBigDecimal(key);
                if(bigDecimal == null){
                    row.isNull(key);
                    break;
                }
                row.addDecimal(key,bigDecimal);
                break;
            case "bool":
                Boolean aBoolean = json.getBoolean(key);
                if(aBoolean == null){
                    row.isNull(key);
                    break;
                }
                row.addBoolean(key,aBoolean);
                break;
            case "double":
                Double doubleValue = json.getDouble(key);
                if(null == doubleValue){
                    row.isNull(key);
                    break;
                }
                row.addDouble(key,doubleValue);
                break;
            case "float":
                Float floatValue = json.getFloat(key);
                if(null == floatValue){
                    row.isNull(key);
                    break;
                }
                row.addFloat(key,floatValue);
                break;
            default:
                row.isNull(key);
                return;
        }
    }
}
