package com.atguigu.mapreduce.writablecompable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: Xionghx
 * @Date: 2022/07/12/9:39
 * @Version: 1.0
 */
public class FlowReducer extends Reducer<FlowBean, Text, Text, FlowBean>
{
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context
            context) throws IOException, InterruptedException {
        //遍历 values 集合,循环写出,避免总流量相同的情况
        for (Text value : values) {
            //调换 KV 位置,反向写出
            context.write(value,key);
        }
    }
}
