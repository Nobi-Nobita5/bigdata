package com.atguigu.mapreduce.reducejoin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
/**
 * @Author: Xionghx
 * @Date: 2022/07/13/16:36
 * @Version: 1.0
 *
 * select t1.id,t2.pname,t1.amount
 * 订单表order t1
 * left join 商品名称表pd t2
 * on t1.pid = t2.pid
 */
public class TableDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(TableDriver.class);
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        FileInputFormat.setInputPaths(job, new Path("F:\\学习笔记\\日常学习笔记\\bigdata\\Hadoop3\\MapReduceDemo\\src\\main\\resources\\inputtable"));
//        FileOutputFormat.setOutputPath(job, new Path("F:\\学习笔记\\日常学习笔记\\bigdata\\Hadoop3\\MapReduceDemo\\src\\main\\resources\\output\\outputtable"));

        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0:1);

    }
}
