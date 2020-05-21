package com.yr.kudu.utils;

import org.apache.commons.lang3.time.DateUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author baiyang
 * @version 1.0
 * @date 2020/5/21 10:22 上午
 */
public class DateUtil {

    public static final String yyyy_MM_dd_HH_mm_ss = "yyyy-MM-dd HH:mm:ss";


    public static String millisecondFormat(Long millisecond, String format){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        Date date = new Date();
        date.setTime(millisecond);
        return simpleDateFormat.format(date);
    }

    public static Long getMillisecond(String str, String format) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        Date parse = simpleDateFormat.parse(str);
        return parse.getTime();
    }
}
