package com.yr.kudu.utils;

import com.alibaba.fastjson.JSONObject;
import com.yr.kudu.arithmetic.KMPArithmetic;
import org.apache.kudu.client.PartialRow;

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author baiyang
 * @version 1.0
 * @date 2020/5/15 4:34 下午
 */
public class KuduUtil {

    public static Map<String,Object> getProperty(Object object)  throws Exception {
        Field[] fields = object.getClass().getDeclaredFields();
        HashMap<String, Object> stringObjectHashMap = new HashMap<>();
        for (Field field : fields) {
            field.setAccessible(true);
            String classType = field.getType().toString();
            int lastIndex = classType.lastIndexOf(".");
            classType = classType.substring(lastIndex + 1);
            stringObjectHashMap.put(field.getName(),"");
            System.out.println("fieldName：" + field.getName() + ",type:"
                    + classType + ",value:" + field.get(object));
        }
        return stringObjectHashMap;
    }

    public static void typeConversion(JSONObject json, PartialRow row, String key, String type) {
        type = type.trim().toLowerCase();
        int decimal = KMPArithmetic.kmp(type, "decimal", new int["decimal".length()]);
        if(decimal == 0){
            type = "decimal";
        }
        switch (type){
            case "integer":
                row.addInt(key,json.getInteger(key));
                break;
            case "varchar":
                row.addString(key,json.getString(key));
                break;
            case "bigint":
                row.addLong(key,json.getLongValue(key));
                break;
            case "decimal":
                row.addDecimal(key,json.getBigDecimal(key));
                break;
            default:
                row.isNull(key);
                return;
        }
    }
}
