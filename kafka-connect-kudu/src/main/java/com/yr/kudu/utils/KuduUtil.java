package com.yr.kudu.utils;

import com.yr.kudu.arithmetic.KMPArithmetic;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.PartialRow;
import org.jetbrains.annotations.NotNull;

import static java.math.BigDecimal.valueOf;

/**
 * @author baiyang
 * @version 1.0
 * @date 2020/5/15 4:34 下午
 */
public class KuduUtil {


    public static final String KUDUDATESTRING = "_kudu_date_string";
    public static final String BYTE = "int8";
    public static final String SHORT = "int16";
    public static final String INT = "int32";
    public static final String LONG = "int64";
    public static final String KUDUDATE = "unixtime_micros";
    public static final String STRING = "string";
    public static final String DECIMAL = "decimal";
    public static final String BOOL = "bool";
    public static final String DOUBLE = "double";
    public static final String FLOAT = "float";

    public static void init(KuduClient client, String tableName) throws Exception {
        ConstantInitializationUtil.initialization(client,tableName);
    }

    /**
     *  mysql数据到kudu数据类型转换
     * @param map mysql原数据 map<列名,值>
     * @param row
     * @param key kudu的列名
     * @param type kudu的类型
     */
    public static void typeConversion(CaseInsensitiveMap map, PartialRow row, @NotNull String key, @NotNull String type) {
        Object value = map.get(key);
        // 判断是否为date，datetime，TIMESTAMP 类型的string列
        if(STRING.equals(type) && -1 != KMPArithmetic.kmp(key,KUDUDATESTRING,new int[KUDUDATESTRING.length()]))
        {
            String tempKey = key.split(KUDUDATESTRING)[0];
            Object tempValue = map.get(tempKey);
            if(tempValue == null){
                row.isNull(key);
                return;
            }
            Long millisecond =Long.parseLong(tempValue.toString());
            String dateString = DateUtil.millisecondFormat(millisecond, "yyyy-MM-dd HH:mm:ss");
            row.addString(key,dateString);
            //为空处理
        } else if(isNullObject(value)) {
            row.isNull(key);
//            类型转换
        } else {
            switch (type){
                case BYTE:
                    row.addByte(key, Byte.parseByte(value.toString()));
                    break;
                case SHORT:
                    row.addShort(key, Short.parseShort(value.toString()));
                    break;
                case INT:
                    row.addInt(key, Integer.parseInt(value.toString()));
                    break;
                case LONG:
                case KUDUDATE:
                    row.addLong(key, Long.parseLong(value.toString()));
                    break;
                case STRING:
                    row.addString(key,value.toString());
                    break;
                case DECIMAL:
                    row.addDecimal(key, valueOf(Double.parseDouble(value.toString())));
                    break;
                case BOOL:
                    row.addBoolean(key, Boolean.parseBoolean(value.toString()));
                    break;
                case DOUBLE:
                    row.addDouble(key, Double.parseDouble(value.toString()));
                    break;
                case FLOAT:
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
