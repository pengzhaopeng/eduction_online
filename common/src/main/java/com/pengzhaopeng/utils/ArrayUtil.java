package com.pengzhaopeng.utils;

/**
 * @author 鹏鹏鹏先森
 * @date 2020/2/24 21:03
 * @Version 1.0
 * @description 数组工具类
 */
public  class ArrayUtil {

    /**
     * 判断数组是否为空
     * @param array
     * @return
     */
    public static boolean isNotEmpty(Object[] array){
        return !isEmpty(array);
    }

    /**
     * 判断数组是否非空
     * @param array
     * @return
     */
    public static boolean isEmpty(Object[] array){
        return array==null||array.length==0;
    }
}
