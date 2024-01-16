package com.atguigu.realtime.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

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
        return LocalDate.parse(dateTime, dateTimeFormatter)
                .atStartOfDay(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
    }

    public static void main(String[] args) {
        System.out.println(parseTsToDate(1704262146000L));
        System.out.println(parseTsToDateTime(1704262146000L));
        System.out.println(dateTimeToTs("2024-01-03 14:09:06"));
        System.out.println(dateToTs("2024-01-03"));
    }
}
