package com.atguigu.mapreduce.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: Xionghx
 * @Date: 2022/07/12/17:28
 * @Version: 1.0
 */
public class LogReducer extends Reducer<Text,NullWritable,Text,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        // 防止有相同的数据被去重,则迭代写出，可以输出每条数据。
        // 相当于是在本案例中，Reduce端对相同key的数据，不做处理，一一输出。
        for (NullWritable value :
                values) {
            context.write(key, NullWritable.get());
        }
    }
}
