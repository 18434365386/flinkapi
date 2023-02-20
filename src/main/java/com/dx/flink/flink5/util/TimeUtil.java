package com.dx.flink.flink5.util;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * 定义时间格式输出类
 */
public class TimeUtil {
    public static  String format(Date time){
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        ZoneId zoneId = ZoneId.systemDefault();

        String timeStr = formatter.format(time.toInstant().atZone(zoneId));
        return timeStr;
    }
}
