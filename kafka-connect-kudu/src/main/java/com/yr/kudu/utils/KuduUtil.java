package com.yr.kudu.utils;

import com.yr.kudu.arithmetic.KMPArithmetic;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.PartialRow;
import org.jetbrains.annotations.NotNull;

import java.text.ParseException;

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

    private static final String T = "T";

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
    public static void typeConversion(@NotNull CaseInsensitiveMap map,@NotNull PartialRow row, @NotNull String key, @NotNull String type) throws ParseException {
        Object value = map.get(key);
        // 判断是否为date，datetime，TIMESTAMP 类型的string列
        if(STRING.equals(type) && -1 != KMPArithmetic.kmp(key,KUDUDATESTRING,new int[KUDUDATESTRING.length()]))
        {
            String tempKey = key.split(KUDUDATESTRING)[0];
            Object tempObject = map.get(tempKey);
            if(tempObject == null){
                row.isNull(key);
                return;
            }
            String tempValue = tempObject.toString();
            // TIMESTAMP 类型 推入数据格式为 2020-02-02T19:19:19Z 特殊处理
            if(-1 != KMPArithmetic.kmp(tempValue,T,new int[T.length()])){
                 tempValue = tempValue.replace("T", " ").replace("Z", "");
                row.addString(key,tempValue);
                return;
            }
            Long millisecond =Long.parseLong(tempValue);
            String dateString = DateUtil.millisecondFormat(millisecond, DateUtil.yyyy_MM_dd_HH_mm_ss);
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
                case KUDUDATE:
                    String dateValue = value.toString();
                    Long millisecond;
                    // TIMESTAMP 类型 推入数据格式为 2020-02-02T19:19:19Z 特殊处理
                    if(-1 != KMPArithmetic.kmp(dateValue,T,new int[T.length()])){
                        dateValue = dateValue.replace("T", " ").replace("Z", "");
                        millisecond = DateUtil.getMillisecond(dateValue, DateUtil.yyyy_MM_dd_HH_mm_ss);
                    } else {
                        millisecond = Long.parseLong(dateValue);
                    }
                    row.addLong(key,millisecond);
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
