package com.pengzhaopeng.utils;


/**
 * 字符串工具类s
 */
public  class StringUtil {

    public static void main(String[] args) {
        System.out.println(StringUtil.class.getClassLoader().getParent().getParent());
    }

    /**
     * 判断是否为空字符串最优代码
     * @param str
     * @return 如果为空，则返回true
     */
    public static boolean isEmpty(String str){
        return str == null || str.trim().length() == 0;
    }

    /**
     * 判断字符串是否非空
     * @param str 如果不为空，则返回true
     * @return
     */
    public static boolean isNotEmpty(String str){
        return !isEmpty(str);
    }
}