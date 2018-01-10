package com.gertent.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description
 * @Author wyf
 * @Date 2018/1/10
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value,  Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //得到输入的每一行数据 hello a
        String line = value.toString();

        //通过空格分割数据，hello,a
        String[] words = line.split(" ");

        //循环遍历输出
        for(String word : words){
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
