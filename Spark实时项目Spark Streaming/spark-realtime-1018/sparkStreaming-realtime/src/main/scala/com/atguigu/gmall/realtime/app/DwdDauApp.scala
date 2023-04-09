package com.atguigu.gmall.realtime.app

import java.text.SimpleDateFormat
import java.time.{LocalDate, Period, ZoneId}
import java.{lang, util}
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.bean.{DauInfo, PageLog}
import com.atguigu.gmall.realtime.util.{MyBeanUtils, MyEsUtils, MyKafkaUtils, MyOffsetsUtils, MyRedisUtils, TimeUtils_Test}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.{Jedis, Pipeline}

import scala.collection.mutable.ListBuffer

/**
  * 日活宽表
  *
  * 1. 准备实时环境
  * 2. 从Redis中读取偏移量
  * 3. 从kafka中消费数据
  * 4. 提取偏移量结束点
  * 5. 处理数据
  *     5.1 转换数据结构
  *     5.2 去重
  *     5.3 维度关联
  * 6. 写入ES
  * 7. 提交offsets
  */
object DwdDauApp {

  def main(args: Array[String]): Unit = {
    //0.还原状态
    revertState()

    //1. 准备实时环境
    val sparkConf: SparkConf = new SparkConf().setAppName("dwd_dau_app").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))

    //2. 从Redis中读取offset
    val topicName : String = "DWD_PAGE_LOG_TOPIC_1018"
    val groupId : String = "DWD_DAU_GROUP"
    val offsets: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(topicName, groupId)

    //3. 从Kafka中消费数据
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsets != null && offsets.nonEmpty){
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc,topicName,groupId,offsets)
    }else {
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc,topicName,groupId)
    }

    //4. 提取offset结束点
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    //5. 处理数据
    // 5.1 转换结构
    val pageLogDStream: DStream[PageLog] = offsetRangesDStream.map(
      consumerRecord => {
        val value: String = consumerRecord.value()
        val pageLog: PageLog = JSON.parseObject(value, classOf[PageLog])
        pageLog
      }
    )

    /**
     * DStream可以理解为一系列连续的RDD，每个RDD都包含了一段时间内的数据。
     * 缓存处理Dstream流，方便打印操作后可以再次操作。因为行动操作触发后，该Dstream流就不存在了
     * 这里解释下：
     *         TODO DStream并非真正"消失"，而是原始数据被消费和处理后，产生了新的RDD,新的DStream。在整个处理过程中，DStream始终存在，只是其中的数据随着操作的执行而发生变化。
     *        相当于行动操作触发后，旧的pageLogDStream流在被处理后会被逐渐取代，产生的新的DStream并不叫pageLogDStream。
     * */
    pageLogDStream.cache()
    pageLogDStream.foreachRDD(
      rdd => println("自我审查前: " + rdd.count())
    )
    //5.2 去重
    // 自我审查: 操作当前批次Dstream流，将页面访问数据中last_page_id不为空的数据过滤掉
    val filterDStream: DStream[PageLog] = pageLogDStream.filter(
      pageLog => pageLog.last_page_id == null
    )
    filterDStream.cache()//缓存处理Dstream流，方便打印操作后可以再次操作。因为行动操作触发后，该Dstream流就不存在了
    filterDStream.foreachRDD(
      rdd => {
        println("自我审查后: " + rdd.count())
        println("----------------------------")
      }
    )

    // 第三方审查:  通过redis将当日活跃的mid维护起来,自我审查后的每条数据需要到redis中进行比对去重
    // redis中如何维护日活状态
    // 类型:    set
    // key :    DAU:DATE
    // value :  mid的集合
    // 写入API: sadd
    // 读取API: smembers
    // 过期:  24小时
    //filterDStream.filter()  // 该算子，每条数据执行一次. redis的连接太频繁.
    // [A, B , C ] => [ AA ,BB ]

    // 此处使用mapPartitions算子实现过滤功能，每批次每分区执行一次
    val redisFilterDStream: DStream[PageLog] = filterDStream.mapPartitions(//mapPartitions和foreachPartition的区别：
                                                                           //前者是转换操作，可以返回新的RDD 或 DStream，
                                                                            //而foreachPartition是action操作，所有不会返回新的 RDD 或 DStream。
      //动作操作（action）通常会触发计算并将结果返回给驱动程序（driver）。与转换操作（transformation）不同，动作操作不会返回新的 RDD 或 DStream，而是返回一个值或者将数据写入到外部存储系统。
      pageLogIter => {
        val pageLogList: List[PageLog] = pageLogIter.toList
        //pageLogIter是个数据流的迭代器，下方代码调用size()，已经进行了一次迭代，该数据流就不存在了。
        //我们后续还得用到该迭代器，所以进行toList操作生成pageLogList
        println("第三方审查前: " + pageLogList.size)

        //存储要的数据
        val pageLogs: ListBuffer[PageLog] = ListBuffer[PageLog]()
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        for (pageLog <- pageLogList) {
          // 提取每条数据中的mid (我们日活的统计基于mid， 也可以基于uid)
          val mid: String = pageLog.mid

          //获取日期 , 因为我们要测试不同天的数据，所以不能直接获取系统时间.
          val ts: Long = pageLog.ts
          val date: Date = new Date(ts)
          val dateStr: String = sdf.format(date)
          val redisDauKey: String = s"DAU:$dateStr"

          //redis的判断是否包含操作
          /*
          下面代码在分布式环境中，存在并发问题， 可能多个并行度同时进入到if中,导致最终保留多条同一个mid的数据.
          // list
          val mids: util.List[String] = jedis.lrange(redisDauKey, 0 ,-1)
          if(!mids.contains(mid)){
            jedis.lpush(redisDauKey , mid )
            pageLogs.append(pageLog)
          }
          // set
          val setMids: util.Set[String] = jedis.smembers(redisDauKey)
          if(!setMids.contains(mid)){
            jedis.sadd(redisDauKey,mid)
            pageLogs.append(pageLog)
          }

           */
          val isNew: lang.Long = jedis.sadd(redisDauKey, mid) // 判断是否包含 和 写入实现了原子操作.
          if (isNew == 1L) {
            val tomorrowMidnight = TimeUtils_Test.tomorrowMidnightTimestamp
            jedis.expireAt(redisDauKey,tomorrowMidnight) //设置超时时间为第二天0点
            pageLogs.append(pageLog)
          }
        }
        jedis.close()
        println("第三方审查后: " + pageLogs.size)
        pageLogs.iterator
      }
    )
    //redisFilterDStream.print()

    //5.3 维度关联
    val dauInfoDStream: DStream[DauInfo] = redisFilterDStream.mapPartitions(
      pageLogIter => {
        val dauInfos: ListBuffer[DauInfo] = ListBuffer[DauInfo]()
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        for (pageLog <- pageLogIter) {
          val dauInfo: DauInfo = new DauInfo()
          //1. 将pagelog中已有的字段拷贝到DauInfo中
          //笨办法: 将pageLog中的每个字段的值挨个提取，赋值给dauInfo中对应的字段。
          //dauInfo.mid = pageLog.mid
          //好办法: 通过对象拷贝来完成.
          MyBeanUtils.copyProperties(pageLog, dauInfo)

          //2. 补充维度
          //2.1  用户信息维度
          val uid: String = pageLog.user_id
          val redisUidkey: String = s"DIM:USER_INFO:$uid"
          val userInfoJson: String = jedis.get(redisUidkey)
          val userInfoJsonObj: JSONObject = JSON.parseObject(userInfoJson)
          //提取性别
          val gender: String = userInfoJsonObj.getString("gender")
          //提取生日
          val birthday: String = userInfoJsonObj.getString("birthday") // 1976-03-22
          //换算年龄
          val birthdayLd: LocalDate = LocalDate.parse(birthday)
          val nowLd: LocalDate = LocalDate.now()
          val period: Period = Period.between(birthdayLd, nowLd)
          val age: Int = period.getYears

          //补充到对象中
          dauInfo.user_gender = gender
          dauInfo.user_age = age.toString

          //2.2  地区信息维度
          // redis中:
          // 现在: DIM:BASE_PROVINCE:1
          val provinceID: String = dauInfo.province_id
          val redisProvinceKey: String = s"DIM:BASE_PROVINCE:$provinceID"
          val provinceJson: String = jedis.get(redisProvinceKey)
          val provinceJsonObj: JSONObject = JSON.parseObject(provinceJson)
          val provinceName: String = provinceJsonObj.getString("name")
          val provinceIsoCode: String = provinceJsonObj.getString("iso_code")
          val province3166: String = provinceJsonObj.getString("iso_3166_2")
          val provinceAreaCode: String = provinceJsonObj.getString("area_code")

          //补充到对象中
          dauInfo.province_name = provinceName
          dauInfo.province_iso_code = provinceIsoCode
          dauInfo.province_3166_2 = province3166
          dauInfo.province_area_code = provinceAreaCode

          //2.3  日期字段处理
          val date: Date = new Date(pageLog.ts)
          val dtHr: String = sdf.format(date)
          val dtHrArr: Array[String] = dtHr.split(" ")
          val dt: String = dtHrArr(0)
          val hr: String = dtHrArr(1).split(":")(0)
          //补充到对象中
          dauInfo.dt = dt
          dauInfo.hr = hr

          dauInfos.append(dauInfo)
        }

        jedis.close()
        dauInfos.iterator
      }
    )
    //dauInfoDStream.print(100)

    //写入到OLAP中
    //按照天分割索引，通过索引模板控制mapping、settings、aliases等.
    //准备ES工具类
    dauInfoDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          dauInfoIter => {
            val docs: List[(String, DauInfo)] =
              dauInfoIter.map( dauInfo=> (dauInfo.mid , dauInfo)).toList
            if(docs.size >  0 ){
              // 索引名
              // 如果是真实的实时环境，直接获取当前日期即可.
              // 因为我们是模拟数据，会生成不同天的数据.
              // 从第一条数据中获取日期
              val head: (String, DauInfo) = docs.head
              val ts: Long = head._2.ts
              val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
              val dateStr: String = sdf.format(new Date(ts))
              val indexName : String = s"gmall_dau_info_1018_$dateStr"
              //写入到ES中
              MyEsUtils.bulkSave(indexName , docs)
            }
          }
        )
        //提交offset
        MyOffsetsUtils.saveOffset(topicName, groupId , offsetRanges)
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 状态还原
    *
    * 在每次启动实时任务时， 进行一次状态还原。 以ES为准, 将所有的mid提取出来，覆盖到Redis中.
    */

  def revertState(): Unit ={
    //从ES中查询到所有的mid
    val date: LocalDate = LocalDate.now()
    val indexName : String = s"gmall_dau_info_1018_$date"
    val fieldName : String = "mid"
    val mids: List[ String ] = MyEsUtils.searchField(indexName , fieldName)
    //删除redis中记录的状态（所有的mid），无论ES中是否有数据，redis中的数据都要删除。
    val jedis: Jedis = MyRedisUtils.getJedisFromPool()
    val redisDauKey : String = s"DAU:$date"
    jedis.del(redisDauKey)
    //将从ES中查询到的mid覆盖到Redis中
    if(mids != null && mids.size > 0 ){
      /*for (mid <- mids) {
        jedis.sadd(redisDauKey , mid )
      }*/
      //mids数据是固定的，没必要一个一个写，以下操作可以实现批次写入。pipline(管道)，将数据先加入到管道中，再批量写入
      val pipeline: Pipeline = jedis.pipelined()
      for (mid <- mids) {
        pipeline.sadd(redisDauKey , mid )  //不会直接到redis执行，数据放入管道
      }

      pipeline.sync()  // 到redis执行
    }

    jedis.close()
  }
}
