package com.gertent.mapreduce.test;

import org.apache.hadoop.conf.Configuration;

/**
 * @Description
 * @Author wyf
 * @Date 2018/1/10
 */
public class ConfigurationTest {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        //资源合并
        conf.addResource("configuration-1.xml");
        conf.addResource("configuration-2.xml");
        System.out.println(conf.get("color") + "," + conf.getInt("size",0) + "," + conf.get("weight"));

    }
}
