package com.pengzhaopeng.utils;

import java.io.*;

/**
 * @author 鹏鹏鹏先森
 * @date 2020/2/24 21:09
 * @Version 1.0
 * @description 文件编码转码
 * 将GBK编码的文件转为UTF-8编码的文件, 经常配合上一个使用，下载的压缩包解压为文件然后解码。
 */
public  class TransferUtil {
    /**
     * 把GBK文件转为UTF-8
     * 两个参数值可以为同一个路径
     * @param srcFileName 源文件
     * @param destFileName 目标文件
     * @throws IOException
     */
    private static void transferFile(String srcFileName, String destFileName) throws IOException {
        String line_separator = System.getProperty("line.separator");
        FileInputStream fis = new FileInputStream(srcFileName);
        StringBuffer content = new StringBuffer();
        DataInputStream in = new DataInputStream(fis);
        BufferedReader d = new BufferedReader(new InputStreamReader(in, "GBK"));  //源文件的编码方式
        String line = null;
        while ((line = d.readLine()) != null)
            content.append(line + line_separator);
        d.close();
        in.close();
        fis.close();

        Writer ow = new OutputStreamWriter(new FileOutputStream(destFileName), "utf-8");  //需要转换的编码方式
        ow.write(content.toString());
        ow.close();
    }
}
