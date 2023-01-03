package com.atguigu.mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: Xionghx
 * @Date: 2022/07/12/14:50
 * @Version: 1.0
 */
public class WordCountCombiner extends Reducer<Text, IntWritable,Text, IntWritable>{
    private IntWritable outV = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        //封装 outKV
        outV.set(sum);
        //写出outKV
        context.write(key,outV);
    }
}
