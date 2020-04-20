package com.pengzhaopeng.utils;

import com.alibaba.fastjson.JSONObject;

/**
 * @author 17688700269
 * @date 2020/3/26 17:18
 * @Version 1.0
 * @description 判断是否是json
 */
public class ParseJsonData {
    public static JSONObject getJsonData(String data) {
        try {
            return JSONObject.parseObject(data);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
