package com.atguigu.mapreduce.reduceJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
/**
 * @Author: Xionghx
 * @Date: 2022/07/13/16:04
 * @Version: 1.0
 */
public class TableReducer extends Reducer<Text,TableBean,TableBean,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
//        01 	1001	1   order
//        01 	1004	4   order
//        01	小米   	     pd
        // 准备初始化集合
        ArrayList<TableBean> orderBeans = new ArrayList<>();
        //相同的key(产品id,关联字段)，order表可能会有多个相同的产品id
        //所以这里设置为ArrayList<TableBean>类型，方便存储多个相同产品id的order信息
        TableBean pdBean = new TableBean();
        //相同的key(产品id,关联字段)，pd表只会有一个产品id
        //所以这里直接设置为TableBean类型

        // 循环遍历
        for (TableBean value : values) {
            //判断数据来自哪个表
            if("order".equals(value.getFlag())){ //订单表
                //创建一个临时 TableBean 对象接收 value
                TableBean tmpOrderBean = new TableBean();
                try {
                    BeanUtils.copyProperties(tmpOrderBean,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                //将临时 TableBean 对象添加到集合 orderBeans
                orderBeans.add(tmpOrderBean);
            }else { //商品表
                try {
                    BeanUtils.copyProperties(pdBean,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        //遍历集合 orderBeans,替换掉每个 orderBean 的 pid 为 pname,然后写出
        for (TableBean orderBean : orderBeans) {

            orderBean.setPname(pdBean.getPname());
            //写出修改后的 orderBean 对象
            context.write(orderBean,NullWritable.get());
        }
    }
}