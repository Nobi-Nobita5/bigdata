package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.gmall.realtime.bean.{PageActionLog, PageDisplayLog, PageLog, StartLog}
import com.atguigu.gmall.realtime.util.{MyKafkaUtils, MyOffsetsUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 日志数据的消费分流
 * 1. 准备实时处理环境 StreamingContext
 *
 * 2. 从Kafka中消费数据
 *
 * 3. 处理数据
 *     3.1 转换数据结构
 *           专用结构  Bean
 *           通用结构  Map JsonObject
 *     3.2 分流
 *
 * 4. 写出到DWD层
 */
object OdsBaseLogApp {
  def main(args: Array[String]): Unit = {
    //1. 准备实时环境
    //TODO 注意并行度与Kafka中topic的分区个数的对应关系
    val sparkConf: SparkConf = new SparkConf().setAppName("ods_base_log_app").setMaster("local[1]")
    val ssc: StreamingContext = new StreamingContext(sparkConf , Seconds(5))

    //2. 从kafka中消费数据
    val topicName : String = "ODS_BASE_LOG_1018"  //对应生成器配置中的主题名
    val groupId : String = "ODS_BASE_LOG_GROUP_1018"

    //TODO  从Redis中读取offset， 指定offset进行消费
    //val offsets: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(topicName, groupId)

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtils.getKafkaDStream(ssc,topicName,groupId)

    //kafkaDStream.print(100)，打印报错
    //此处的ConsumerRecord不支持Serializable接口，如果需要获取DStream数据打印并分流传递，则需要转换数据结构
    //3. 处理数据
    //3.1 转换数据结构
    val jsonObjDStream: DStream[JSONObject] = kafkaDStream.map(
      consumerRecord => {
        //获取ConsumerRecord中的value,value就是日志数据
        val log: String = consumerRecord.value()
        //转换成Json对象
        val jsonObj: JSONObject = JSON.parseObject(log)
        //返回
        jsonObj
      }
    )
     jsonObjDStream.print(1000) //处理流数据打印出json数据

    ssc.start()
    /*等待终止*/
    ssc.awaitTermination()
  }
}
