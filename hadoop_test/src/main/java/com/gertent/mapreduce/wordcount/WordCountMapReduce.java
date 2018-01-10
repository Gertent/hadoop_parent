package com.gertent.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Description
 * @Author wyf
 * @Date 2018/1/10
 */
public class WordCountMapReduce {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建配置对象
        Configuration conf = new Configuration();

        //创建Job对象
        Job job = Job.getInstance(conf, "wordcount");

        //设置Job运行的主类
        job.setJarByClass(WordCountMapReduce.class);

        //设置mapper类
        job.setMapperClass(WordCountMapper.class);
        //设置reduce类
        job.setReducerClass(WordCountReducer.class);

        //设置map输出的key,value
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置reduce输出的key,value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:9000/test/words.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/test/out06"));

        //提交job
        boolean b = job.waitForCompletion(true);
        if(!b){
            System.err.println("This task has failed");
        }

    }
}
