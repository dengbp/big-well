package com.yr.kudu.utils;

import com.yr.kudu.constant.SessionPool;
import com.yr.kudu.constant.TableTypeConstantMap;
import org.apache.commons.io.IOUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;

/**
 * @author baiyang
 * @version 1.0
 * @date 2020/5/16 10:14 上午
 */
public class ConstantInitializationUtil {
    private static final String ALL = "all";

    public static void initialization(String modelName) throws Exception {
        setProperty(TableTypeConstantMap.getTableTypeConstantMap(),readFile(),modelName);
        SessionPool.initSessionPool();
    }

    public static Map<String,Object> readFile() throws IOException {
        Map<String, Object> result;
        File file = new File("/data/platform/confluent/confluent-5.4.1/share/java/kafka-connect-kudu/tableMessage.yaml");
        FileInputStream fileInputStream = new FileInputStream(file);
        String s = IOUtils.toString(fileInputStream,"UTF-8");
        Yaml yaml = new Yaml();
        result = yaml.load(s);
        return result;
    }

    /**
     * 更新static对象中表的信息
     * @param object 常量表
     * @param map yaml文件中的信息
     * @param modelName 指定更新模块表 ALL 代表所有模块 指定模块的名称与yaml文件的表名，TableTypeConstantMap 中属性名要三者一致
     * @throws Exception
     */
    public static void setProperty(Object object,Map<String, Object> map, String modelName)  throws Exception {
        // 通过反射获取表的属性
        Field[] fields = object.getClass().getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
            if(null != map.get(field.getName())){
                if(ALL.equals(modelName)){
                    field.set(object,map.get(field.getName()));
                } else if(field.getName().equals(modelName)){
                    field.set(object,map.get(field.getName()));
                }
            }
        }
    }
}
