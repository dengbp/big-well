package com.yr.net.collect;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author dengbp
 * @ClassName CollectManager
 * @Description TODO
 * @date 2020-11-19 18:18
 */
public class CollectManager {

      private static Map<CollectType,Collector> collectors = new HashMap<>(1);

      public static boolean register(CollectType type) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
          collectors.put(type,instance(type));
          return true;
      }


      private static Collector instance(CollectType type) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
          Class[] paramTypes = {CollectType.class};
          Constructor constructor = type.getTClass().getConstructor(paramTypes);
          return (Collector) constructor.newInstance(type);
      }

      public static Collector getCollector(CollectType type){
          return collectors.get(type);
      }


}
