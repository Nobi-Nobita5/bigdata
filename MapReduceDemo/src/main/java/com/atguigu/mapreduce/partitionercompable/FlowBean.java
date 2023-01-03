package com.atguigu.mapreduce.partitionercompable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author: Xionghx
 * @Date: 2022/07/12/9:18
 * @Version: 1.0
 */
//1 继承 Writable 接口
public class FlowBean implements WritableComparable<FlowBean> {
    private  long upFlow;
    private  long downFlow;
    private  long sumFlow;
    //2 提供无参构造
    public FlowBean(){

    }
    //3 提供三个参数的 getter 和 setter 方法
    public long getUpFlow(){
        return upFlow;
    }

    public void setUpFlow(long upFlow){
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow() {
        this.sumFlow = this.upFlow + this.downFlow;
    }
    //4 实现序列化和反序列化方法,注意顺序一定要保持一致
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();
    }

    //5 重写ToString
    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    /**按照总流量比较,倒序排列
     *在重写方法compareto()的是时候，会传对象，我们这里称其为比较对象，当前类为当前对象，如下：
     *当前对象=比较对象，则返回0；当前对象＞比较对象，则返回1；当前对象＜比较对象，则返回-1；这样是升序排序的。
     *当前对象=比较对象，则返回0；当前对象＞比较对象，则返回-1；当前对象＜比较对象，则返回1；这样是降序排序的
     * @param o
     * @return
     */
    @Override
    public int compareTo(FlowBean o) {
        if(this.sumFlow > o.sumFlow){
            return -1;
        }else if(this.sumFlow < o.sumFlow){
            return 1;
        }else return 0;
    }
}
