package com.gertent.hdfs.test;

import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

/**
 * 以URL方式读取hadoop中的文件
 * @author wyf
 * @date 2018/1/9
*/
public class AppDemo1 {
    //hadoop中文件的路径
    private static final String HDFS_PATH = "hdfs://localhost:9000/testHadoop/server.xml";
    public static void main(String[] args) throws Exception {
        //给URL配置解析器，使其能解析hdfs协议
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        URL url = new URL(HDFS_PATH);
        InputStream in = url.openStream();
        //将文件的内容copy到控制台
        IOUtils.copyBytes(in, System.out, 1024, true);
    }
}
