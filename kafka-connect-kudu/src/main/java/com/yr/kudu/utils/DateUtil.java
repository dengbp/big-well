package com.yr.kudu.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author baiyang
 * @version 1.0
 * @date 2020/5/21 10:22 上午
 */
public class DateUtil {

    public static String millisecondFormat(Long millisecond, String format){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        Date date = new Date();
        date.setTime(millisecond);
        return simpleDateFormat.format(date);
    }
}
