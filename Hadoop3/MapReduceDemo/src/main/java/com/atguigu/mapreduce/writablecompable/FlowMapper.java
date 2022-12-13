package com.atguigu.mapreduce.writablecompable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: Xionghx
 * @Date: 2022/07/12/9:31
 * @Version: 1.0
 */
public class FlowMapper extends Mapper<LongWritable, Text,FlowBean, Text> {
    private FlowBean outK = new FlowBean();
    private Text outV = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //1 获取一行数据
        String line = value.toString();
        //2 按照"\t",切割数据
        String[] split = line.split("\t");
        //3 封装 outK outV
        outK.setUpFlow(Long.parseLong(split[1]));
        outK.setDownFlow(Long.parseLong(split[2]));
        outK.setSumFlow();
        outV.set(split[0]);
        //4 写出 outK outV
        context.write(outK,outV);
    }

}
