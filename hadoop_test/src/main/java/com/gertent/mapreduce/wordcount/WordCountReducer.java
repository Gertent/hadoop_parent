package com.gertent.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description
 * @Author wyf
 * @Date 2018/1/10
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
        Integer count = 0;
        for (IntWritable value : values){
            count += value.get();
        }
        context.write(key, new IntWritable(count));
    }
}
