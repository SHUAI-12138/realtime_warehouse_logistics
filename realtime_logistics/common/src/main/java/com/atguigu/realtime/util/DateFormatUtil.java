package com.atguigu.realtime.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

public class DateFormatUtil {

    public static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyy-MM-dd HH:mm:ss");
    public static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /**
     * 将 毫秒 时间戳 转换为 yyyy-MM-dd 的时间格式
     * @param ts 毫秒时间戳
     * @return yyyy-MM-dd
     */
    public static String parseTsToDate(long ts) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.systemDefault());
        return dateFormatter.format(localDateTime);
    }

    /**
     * 将 毫秒 时间戳 转换为 yyyy-MM-dd HH:mm:ss 的时间格式
     * @param ts 毫秒时间戳
     * @return yyyy-MM-dd HH:mm:ss
     */
    public static String parseTsToDateTime(long ts) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.systemDefault());
        return dateTimeFormatter.format(localDateTime);
    }

    /**
     * 将 yyyy-MM-dd 转换为 毫秒时间戳
     * @param date yyyy-MM-dd
     * @return 毫秒时间戳
     */
    public static long dateToTs(String date) {
        return LocalDate.parse(date, dateFormatter)
                .atStartOfDay(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
    }

    /**
     * 将 yyyy-MM-dd HH:mm:ss 格式的时间转换成 毫秒时间戳
     * @param dateTime yyyy-MM-dd HH:mm:ss
     * @return 毫秒时间戳
     */
    public static long dateTimeToTs(String dateTime) {
        return LocalDateTime.parse(dateTime, dateTimeFormatter)
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
    }

    private static final long minute = 60;
    private static final long hour = 3600;

    /**
     * 将 时:分:秒 格式的时间转换为 long
     * @param time 时间
     * @param type 转换为 SECONDS 或者 MILLISECONDS
     * @return 返回 long
     */
    public static long timeToTs(String time, TimeUnit type) {
        String[] split = time.split(":");
        if(split.length != 3) throw new RuntimeException("type of time is error!");
        long res = Long.parseLong(split[0]) * hour + Long.parseLong(split[1]) * minute + Long.parseLong(split[2]);
        return type.equals(TimeUnit.SECONDS) ? res : res * 1000;
    }

    /**
     * 将 long 类型 的 ts 转换为 时:分:秒 格式的时间
     * @param ts 毫秒/秒 时间值
     * @param type 传入的时间格式 SECONDS / MILLISECONDS
     * @return 返回 时:分:秒 格式的时间 字符串
     */
    public static String tsToTime(long ts, TimeUnit type) {
        if(type.equals(TimeUnit.MILLISECONDS)) ts /= 1000;
        long hours = ts / hour;
        ts %= hour;
        long minutes = ts / minute;
        ts %= minute;
        String res = "";
        if(hours < 10) res += "0";
        res += hours + ":";
        if(minutes < 10) res += "0";
        res += minutes + ":";
        if(ts < 10) res += "0";
        res += ts;
        return res;
    }

    public static void main(String[] args) {
        System.out.println(parseTsToDate(1704262146000L));
        System.out.println(parseTsToDateTime(1704262146000L));
        System.out.println(dateTimeToTs("2024-01-16 22:00:49"));
        System.out.println(dateToTs("2024-01-16"));
        System.out.println(tsToTime(17042000L, TimeUnit.MILLISECONDS));
        System.out.println(tsToTime(17042L, TimeUnit.SECONDS));
        System.out.println(timeToTs("04:44:02", TimeUnit.SECONDS));
        System.out.println(timeToTs("04:44:02", TimeUnit.MILLISECONDS));
    }
}
