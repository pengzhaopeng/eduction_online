package com.pengzhaopeng.utils;

import org.joda.time.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * https://www.cnblogs.com/Jacian/p/11120241.html
 */
public class JodaTimeUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(JodaTimeUtil.class);

    public static void main(String[] args) {
//        System.out.println(formatDateToString(1577030400000L));
        System.out.println(getCurrentTimeStr(null));
    }

    private JodaTimeUtil() {
    }

    static {
        DateTimeZone.setDefault(DateTimeZone.forID("Asia/Shanghai"));
    }

    private static final String defaultFormat = "yyyy-MM-dd HH:mm:ss";

    /**
     * 获取字符串的时间戳（毫秒级）
     */
    public static long getStrTimeMillis(String timeStr) {
        if (StringUtil.isEmpty(timeStr)) return -1;
        DateTime dateTime = new DateTime(timeStr);
        return dateTime.getMillis();
    }

    /**
     * 获取当前系统的时间（毫秒级）
     *
     * @return
     */
    public static long getCurrentSecondMillis() {
        return DateTimeUtils.currentTimeMillis();
    }

    /**
     * 获取当前系统的时间（字符串）
     *
     * @return
     */
    public static String getCurrentTimeStr(String pattern) {
        if (StringUtil.isEmpty(pattern)) {
            pattern = "yyyy-MM-dd HH:mm:ss";
        }
        SimpleDateFormat df = new SimpleDateFormat(pattern);//设置日期格式
        return df.format(new Date());
    }

    /**
     * 获取当天的开始时间
     *
     * @return
     */
    public static long getStartOfDay() {
        return getStartOfDay(new Date());
    }

    /**
     * 获取某天的开始时间
     *
     * @param date
     * @return
     */
    public static long getStartOfDay(Date date) {
        DateTime dateTime = new DateTime(date);
        DateTime startOfDay = dateTime.withTimeAtStartOfDay();
        return startOfDay.getMillis();
    }

    /**
     * 获取当天的结束时间
     *
     * @return
     */
    public static long getEndOfDay() {
        return getEndOfDay(new Date());
    }

    /**
     * 获取某天的结束时间
     *
     * @param date
     * @return
     */
    public static long getEndOfDay(Date date) {
        DateTime dateTime = new DateTime(date);
        DateTime endOfDay = dateTime.millisOfDay().withMaximumValue();
        return endOfDay.getMillis();
    }

    /**
     * 获取现在距离今天结束还有多长时间
     *
     * @return
     */
    public static long endOfToday() {
        DateTime nowTime = new DateTime();
        DateTime endOfDay = nowTime.millisOfDay().withMaximumValue();
        return endOfDay.getMillis() - nowTime.getMillis();
    }

    /**
     * 计算两个日期的相隔天数
     *
     * @param startTime
     * @param endTime
     * @return
     */
    public static int getBetweenDay(long startTime, long endTime) {
        DateTime startDay = new DateTime(startTime);
        DateTime endDay = new DateTime(endTime);
        return Days.daysBetween(startDay, endDay).getDays();
    }

    /**
     * 对比两个时间是否同一天
     *
     * @param startTime
     * @param endTime
     * @return
     */
    public static boolean isEqualsSameDay(long startTime, long endTime) {
        DateTime startDay = new DateTime(startTime);
        DateTime endDay = new DateTime(endTime);
        Period p = new Period(startDay, endDay, PeriodType.days());
        int days = p.getDays();
        return days == 0;
    }

    /**
     * 生成指定的时间
     *
     * @param year  年
     * @param month 月
     * @param day   日
     * @param hour  时
     * @param min   分
     * @param sec   秒
     * @param msec  毫秒
     * @return
     */
    public static Date generateDate(int year, int month, int day, int hour, int min, int sec, int msec) {
        DateTime dt = new DateTime(year, month, day, hour, min, sec, msec);
        return dt.toDate();
    }

    public static Date generateDate(int year, int month, int day, int hour, int min, int sec) {
        DateTime dt = new DateTime(year, month, day, hour, min, sec);
        return dt.toDate();
    }

    public static Date generateDate(int year, int month, int day) {
        LocalDate localDate = new LocalDate(year, month, day);
        return localDate.toDate();
    }

    public static Date generateDateTime(int hour, int min, int sec, int msec) {
        LocalTime localTime = new LocalTime(hour, min, sec, msec);
        return localTime.toDateTimeToday().toDate();
    }

    public static Date generateDateTime(int hour, int min, int sec) {
        LocalTime localTime = new LocalTime(hour, min, sec);
        return localTime.toDateTimeToday().toDate();
    }

    /**
     * 格式化日期，转化为string
     *
     * @param date
     * @param format
     * @return
     */
    public static String formatDateToString(Date date, String format) {
        if (StringUtil.isEmpty(format)) format = defaultFormat;
        DateTime dt = new DateTime(date);
        return dt.toString(format);
    }

    public static String formatDateToString(long time, String format) {
        if (StringUtil.isEmpty(format)) format = defaultFormat;
        DateTime dt = new DateTime(time);
        return dt.toString(format);
    }

    /**
     * 格式化日期，转化为string（默认为：yyyy-MM-dd HH:mm:ss）
     *
     * @param date
     * @return
     */
    public static String formatDateToString(Date date) {
        return formatDateToString(date, defaultFormat);
    }

    public static String formatDateToString(long time) {
        return formatDateToString(time, defaultFormat);
    }

}