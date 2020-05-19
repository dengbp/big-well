package com.yr.kudu.utils;

import com.alibaba.fastjson.JSONObject;
import com.yr.kudu.session.SessionManager;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.PartialRow;

import java.math.BigDecimal;

/**
 * @author baiyang
 * @version 1.0
 * @date 2020/5/15 4:34 下午
 */
public class KuduUtil {

    public static void init(KuduClient client,String tableName) throws Exception {
        ConstantInitializationUtil.initialization(client,tableName);
    }

    public static void typeConversion(JSONObject json, PartialRow row, @org.jetbrains.annotations.NotNull String key, String type) {
        json.keySet().forEach(k->k.toLowerCase());
        switch (type){
            case "int8":
                Byte byteValue = json.getByte(key);
                if (isNullObject(byteValue)) {
                    row.isNull(key);
                } else {
                    row.addByte(key, byteValue);
                }
                break;
            case "int16":
                Short aShort = json.getShort(key);
                if (isNullObject(aShort)) {
                    row.isNull(key);
                } else {
                    row.addShort(key, aShort);
                }
                break;
            case "int32":
                Integer integer = json.getInteger(key);
                if (isNullObject(integer)) {
                    row.isNull(key);
                } else {
                    row.addInt(key, integer);
                }
                break;
            case "int64":
                Long longValue = json.getLong(key);
                if (isNullObject(longValue)) {
                    row.isNull(key);
                } else {
                    row.addLong(key, longValue);
                }
                break;
            case "string":
                String string = json.getString(key);
                if (isNullObject(string)) {
                    row.isNull(key);
                } else {
                row.addString(key,string);
                }
                break;
            case "decimal":
                BigDecimal bigDecimal = json.getBigDecimal(key);
                if (isNullObject(bigDecimal)) {
                    row.isNull(key);
                } else {
                    row.addDecimal(key, bigDecimal);
                }
                break;
            case "bool":
                Boolean aBoolean = json.getBoolean(key);
                if (isNullObject(aBoolean)) {
                    row.isNull(key);
                } else {
                    row.addBoolean(key, aBoolean);
                }
                break;
            case "double":
                Double doubleValue = json.getDouble(key);
                if (!isNullObject(doubleValue)) {
                    row.addDouble(key, doubleValue);
                } else {
                    row.isNull(key);
                }
                break;
            case "float":
                Float floatValue = json.getFloat(key);
                if (isNullObject(floatValue)) {
                    row.isNull(key);
                } else {
                    row.addFloat(key, floatValue);
                }
                break;
            default:
                row.isNull(key);
                return;
        }
    }

    public static boolean isNullObject(Object source){
        if(null == source){
            return true;
        } else {
            return false;
        }
    }
}
