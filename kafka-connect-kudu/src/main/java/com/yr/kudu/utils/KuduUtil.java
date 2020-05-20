package com.yr.kudu.utils;

import com.alibaba.fastjson.JSONObject;
import com.yr.kudu.session.SessionManager;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.PartialRow;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;

import static java.math.BigDecimal.valueOf;

/**
 * @author baiyang
 * @version 1.0
 * @date 2020/5/15 4:34 下午
 */
public class KuduUtil {


    public static void init(KuduClient client, String tableName) throws Exception {
        ConstantInitializationUtil.initialization(client,tableName);
    }

    public static void typeConversion(CaseInsensitiveMap map, PartialRow row, @NotNull String key, @NotNull String type) {
        Object value = map.get(key);
        if(isNullObject(value)){
            row.isNull(key);
        } else {
            switch (type){
                case "int8":
                    row.addByte(key, Byte.parseByte(value.toString()));
                    break;
                case "int16":
                    row.addShort(key, Short.parseShort(value.toString()));
                    break;
                case "int32":
                    row.addInt(key, Integer.parseInt(value.toString()));
                    break;
                case "int64":
                    row.addLong(key, Long.parseLong(value.toString()));
                    break;
                case "string":
                    row.addString(key,value.toString());
                    break;
                case "decimal":
                    row.addDecimal(key, valueOf(Double.parseDouble(value.toString())));
                    break;
                case "bool":
                    row.addBoolean(key, Boolean.parseBoolean(value.toString()));
                    break;
                case "double":
                    row.addDouble(key, Double.parseDouble(value.toString()));
                    break;
                case "float":
                    row.addFloat(key, Float.parseFloat(value.toString()));
                    break;
                default:
                    row.isNull(key);
            }
        }
    }

    public static boolean isNullObject(Object source){
        return null == source;
    }
}
