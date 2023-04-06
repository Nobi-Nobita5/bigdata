package com.atguigu.gmall.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
  * Offset管理工具类， 用于往redis中存储和读取offset
  *
  * 管理方案:
  *   1.  后置提交偏移量  ->  手动控制偏移量提交
  *   2.  手动控制偏移量提交 ->  SparkStreaming提供了手动提交方案，但对DStream的结构进行转换之后，就不能获取offset：
  *        （kafka 0.9版本以后consumer的偏移量是保存在kafka的_consumer_offsets主题中,
 *            可以使用xxDstream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)提交偏移量
 *            但有一个限制，即提交偏移量时，数据流也必须是 InputDStream[ConsumerRecord[String, String]]这种结构。
 *            另外，_consumer_offsets 主题的数据格式是由 Kafka 自身维护的，理论上不应该在应用程序中直接操作该主题。）。
 *            事实上，本项目中我们是会将ConsumerRecord转换成JSONObject，所以实际生产中通常会利用 ZooKeeper,Redis,Mysql 等工具手动对偏移量进行保存
  *   3.  手动的提取偏移量维护到redis中
 *          -> 从kafka中消费数据之前，先到redis中读取偏移量， 使用读取到的偏移量到kakfa中消费数据
  *         -> 从kafka中消费到数据,
  *         -> 等数据成功写出后，将偏移量存储到redis中
  *
  *   4. 手动的将偏移量存储到redis中，每次消费数据需要使用存储的offset进行消费，每次消费数据后，要将本次消费的offset存储到redis中。
  */
object MyOffsetsUtils {

  /**
    * 往Redis中存储offset
    * 问题： 存的offset从哪来？
    *            从消费到的数据中提取出来的，传入到该方法中。
    *            offsetRanges: Array[OffsetRange]
    *        offset的结构是什么？
    *            Kafka中offset维护的结构
    *               groupId + topic + partition => offset
    *            从传入进来的offset中提取关键信息
    *        在redis中怎么存?
    *          类型: hash
    *          key : topic + groupId
    *          value: partition - offset  ， partition - offset 。。。。
    *          写入API: hset / hmset
    *          读取API: hgetall
    *          是否过期: 不过期
    */
  def saveOffset( topic : String , groupId : String , offsetRanges: Array[OffsetRange]  ): Unit ={
    if(offsetRanges != null && offsetRanges.length > 0){
      //用HashMap存放redis中hash类型的value。
      val offsets: util.HashMap[String, String] = new util.HashMap[String,String]()
      for (offsetRange <- offsetRanges) {
        val partition: Int = offsetRange.partition
        val endOffset: Long = offsetRange.untilOffset //拿到每个分区的最新偏移量endOffset
        offsets.put(partition.toString,endOffset.toString)
      }
      println("提交offset: " + offsets)
      //往redis中存
      val jedis: Jedis = MyRedisUtils.getJedisFromPool()
      val redisKey : String = s"offsets:$topic:$groupId"
      jedis.hset(redisKey , offsets)
      jedis.close()
    }
  }

  /**
    * 从Redis中读取存储的offset
    *
    * 问题:
    *    如何让SparkStreaming通过指定的offset进行消费?
    *
    *    SparkStreaming要求的offset的格式是什么?
    *                Map[TopicPartition ,Long  ]
    */

  def readOffset(topic: String, groupId : String ):  Map[TopicPartition ,Long  ] ={
    val jedis: Jedis = MyRedisUtils.getJedisFromPool()
    //在字符串 "offsets:" 后插入变量的值
    val redisKey : String = s"offsets:$topic:$groupId"
    val offsets: util.Map[String, String] = jedis.hgetAll(redisKey)
    println("读取到offset: " + offsets)
    val results: mutable.Map[TopicPartition, Long] = mutable.Map[TopicPartition ,Long ]()
    //将java的map转换成scala的map进行迭代
    import scala.collection.JavaConverters._
    for ((partition,offset) <- offsets.asScala) {
      val tp: TopicPartition = new TopicPartition(topic,partition.toInt)
      results.put(tp, offset.toLong)
    }
    jedis.close()
    results.toMap
  }

}
