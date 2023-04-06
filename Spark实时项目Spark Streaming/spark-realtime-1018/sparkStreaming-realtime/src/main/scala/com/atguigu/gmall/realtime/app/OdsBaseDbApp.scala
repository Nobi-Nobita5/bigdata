package com.atguigu.gmall.realtime.app

import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.util.{MyKafkaUtils, MyOffsetsUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * 业务数据消费分流
  *
  * 1. 准备实时环境
  *
  * 2. 从redis中读取偏移量
  *
  * 3. 从kafka中消费数据
  *
  * 4. 提取偏移量结束点
  *
  * 5. 数据处理
  *     5.1 转换数据结构
  *     5.2 分流
  *         事实数据 => Kafka
  *         维度数据 => Redis
  * 6. flush Kafka的缓冲区
  *
  * 7. 提交offset
  *
  *
  */
object OdsBaseDbApp {
  def main(args: Array[String]): Unit = {
    //1. 准备实时环境
    val sparkConf: SparkConf = new SparkConf().setAppName("ods_base_db_app").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(sparkConf , Seconds(5))

    val topicName : String = "ODS_BASE_DB_1018"
    val groupId : String = "ODS_BASE_DB_GROUP_1018"

    //2. 从redis中读取偏移量
    val offsets: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(topicName, groupId)

    //3. 从Kafka中消费数据
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsets != null && offsets.nonEmpty){
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc,topicName,groupId,offsets)
    }else{
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc,topicName,groupId)
    }

    //4. 提取偏移量结束点
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream : DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    //5. 处理数据
    // 5.1 转换数据结构
    val jsonObjDStream: DStream[JSONObject] = offsetRangesDStream.map(
      consumerRecord => {
        val dataJson: String = consumerRecord.value()
        val jSONObject: JSONObject = JSON.parseObject(dataJson)
        jSONObject
      }
    )
    //jsonObjDStream.print(100)

    //5.2 分流


    //事实表清单
    //val factTables : Array[String] = Array[String]( "order_info","order_detail" /*缺啥补啥*/)
    //维度表清单
    //val dimTables : Array[String] = Array[String]("user_info", "base_province" /*缺啥补啥*/)

    //Redis连接写到哪里???
    // foreachRDD外面:  driver ，程序启动获取一次连接，TODO Redis连接对象（如Jedis）通常无法被序列化，不能传输
    // foreachRDD里面, foreachPartition外面 : driver ，每批次获取一次连接，连接对象不能序列化，不能传输
    // foreachPartition里面, jsonObjIter迭代器循环外面：executor ， 每分区数据开启一个连接，用完关闭。选用此方案
    // foreachPartition里面, jsonObjIter迭代器循环里面: executor ， 每条数据开启一个连接，用完关闭，太频繁。
    //
    jsonObjDStream.foreachRDD(
      rdd => {//操作每批次的每个RDD抽象，用foreachPartition每批次每分区执行一次
        //如何动态配置表清单???
        // 将表清单维护到redis中，实时任务中动态的到redis中获取表清单.
        // 类型: set
        // key:  FACT:TABLES   DIM:TABLES
        // value : 表名的集合
        // 写入API: sadd
        // 读取API: smembers
        // 过期: 不过期

        val redisFactKeys : String = "FACT:TABLES"
        val redisDimKeys : String = "DIM:TABLES"
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        //事实表清单
        val factTables: util.Set[String] = jedis.smembers(redisFactKeys)
        println("factTables: " + factTables)
        //做成广播变量
        val factTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(factTables)

        //维度表清单
        val dimTables: util.Set[String] = jedis.smembers(redisDimKeys)
        println("dimTables: " + dimTables)
        //做成广播变量,extends Serializable,可以在各个executor之间传播，不过util.Set也是可以序列化的。
        //    广播变量：共享只读变量。
        //             非广播变量会传递到executor中的每个task中。
        //             广播变量只会传递到每个executor中。供executor中的所有task读取。
        //             在该变量数据量很大的时候，可以明显减少传递变量的网络带宽消耗。
        val dimTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dimTables)
        jedis.close()

        rdd.foreachPartition(
          jsonObjIter => {
            // 开启redis连接，executor端执行，每批次每分区执行一次。
            val jedis: Jedis = MyRedisUtils.getJedisFromPool()
            for (jsonObj <- jsonObjIter) {
              // 提取操作类型，maxwell监控采集来的Json格式数据，其中type表示该条数据的操作类型
              val operType: String = jsonObj.getString("type")

              val opValue: String = operType match { //模式匹配
                case "bootstrap-insert" => "I"
                case "insert" => "I"
                case "update" => "U"
                case "delete" => "D"
                case _ => null
              }
              //判断操作类型: 1. 明确什么操作  2. 过滤不感兴趣的数据
              if(opValue != null){
                // 提取表名
                val tableName: String = jsonObj.getString("table")

                if(factTablesBC.value.contains(tableName)){
                  //事实数据
                  // 提取数据
                  val data: String = jsonObj.getString("data")
                  // DWD_ORDER_INFO_I  DWD_ORDER_INFO_U  DWD_ORDER_INFO_D
                  val dwdTopicName : String = s"DWD_${tableName.toUpperCase}_${opValue}_1018"
                  MyKafkaUtils.send(dwdTopicName ,  data )

                  //模拟数据延迟
                  if(tableName.equals("order_detail")){
                    Thread.sleep(200)
                  }
                }

                if(dimTablesBC.value.contains(tableName)){
                  //维度数据
                  // 类型 : list，set，zset : key为表名，多个value为一条数据，不可行，因为不方便定位每一条数据。
                  //                          key为主键id，多个value为每个字段的数据，不可行，因为不方便定位每个字段的数据。
                  //                          故集合与列表都不合适。
                  //        hash ： 整个表存成一个hash。key是表名，value是主键id和一条数据。 要考虑目前单表数据量大小和将来数据量增长问题 及 高频访问问题。一个key是存放在redis集群中的一个节点上的。
                  //        hash :  一条数据存成一个hash。 key是主键id，value是字段名和数据。没有单独调用某个字段的场景，整查一条数据需要解析很多个field(字段)。
                  //        String : 一条数据存成一个jsonString. key是主键id，value是json字符串。
                  //                 不用担心单表数据量过大，可以让redis集群负载均衡；整查一条数据也比hash方便。
                  //                 故最终采用String方案。
                  // key :  DIM:表名:主键ID
                  // value : 整条数据的jsonString
                  // 写入API: set
                  // 读取API: get
                  // 过期:  不过期（维度数据）
                  //提取数据中的id
                  val dataObj: JSONObject = jsonObj.getJSONObject("data")
                  val id: String = dataObj.getString("id")
                  val redisKey : String = s"DIM:${tableName.toUpperCase}:$id"
                  // 在此处开关redis的连接太频繁.
                  //val jedis: Jedis = MyRedisUtils.getJedisFromPool()
                  jedis.set(redisKey, dataObj.toJSONString)
                  //jedis.close()
                }
              }
            }
            //关闭redis连接
            jedis.close()
            //刷新Kafka缓冲区
            MyKafkaUtils.flush()
          }
        )
        //提交offset
        MyOffsetsUtils.saveOffset(topicName, groupId,offsetRanges)
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }
}















