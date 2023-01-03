package com.bigdata.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @Author: Xionghx
 * @Date: 2022/08/02/14:34
 * @Version: 1.0
 */
public class SimpleProducer1 {
    public static void main(String[] args) {
        String topicName = "first";
        Properties props = new Properties();
        props.put("bootstrap.servers","hadoop102:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);

//        for (int i = 0; i < 10; i++) {
//            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "hello" + i,
//                    "world" + i);
//            /* 发送消息*/
//            producer.send(record);
//        }

//        for (int i = 0; i < 10; i++) {
//            try {
//                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "k" + i, "world" + i);
//                /*同步发送消息*/
//                RecordMetadata metadata = producer.send(record).get();
//                System.out.printf("topic=%s, partition=%d, offset=%s \n",
//                        metadata.topic(), metadata.partition(), metadata.offset());
//            } catch (InterruptedException | ExecutionException e) {
//                e.printStackTrace();
//            }
//        }

        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "k" + i, "world" + i);
            /*异步发送消息，并监听回调*/
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        System.out.println("进行异常处理");
                    } else {
                        System.out.printf("topic=%s, partition=%d, offset=%s \n",
                                metadata.topic(), metadata.partition(), metadata.offset());
                    }
                }
            });
        }
        /*关闭生产者*/
        producer.close();
    }

}
