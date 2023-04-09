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
    val sparkConf: SparkConf = new SparkConf().setAppName("ods_base_log_app").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(sparkConf , Seconds(5))

    val topicName : String = "ODS_BASE_LOG_1018"  //对应生成器配置中的主题名
    val groupId : String = "ODS_BASE_LOG_GROUP_1018"

    //2. 从redis中读取偏移量
    //TODO  从Redis中读取offset， 指定offset进行消费
    val offsets: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(topicName, groupId)

    //3. 从Kafka中消费数据
    //TODO 在Spark Streaming中，DStream（Discretized Stream）是一种高级抽象，它表示一个连续的数据流。DStream可以理解为一系列连续的RDD（Resilient Distributed Dataset），每个RDD都包含了一段时间内的数据。
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsets != null && offsets.nonEmpty ){
      //指定offset进行消费
      kafkaDStream=
        MyKafkaUtils.getKafkaDStream(ssc, topicName , groupId , offsets)
    }else{
      //默认offset进行消费
      kafkaDStream=
        MyKafkaUtils.getKafkaDStream(ssc, topicName , groupId )
    }
    //4. 提取偏移量结束点
    // TODO 补充: 不对DStream流中的数据做任何处理。只是通过如下代码从当前消费到的数据流kafkaDStream中提取offsets
    // TODO 本批次流数据offsetRangesDStream已获取完毕。在对当前流进行处理之前，拿到【本批次】的偏移量信息。在数据写出之后，将该偏移量信息提交（即写入Redis保存）
    var  offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      //具体是通过将kafkaDStream的rdd由ConsumerRecord[String, String]类型 转换为 HasOffsetRanges类型 的特征，因为HasOffsetRanges有获取偏移量的offsetRanges方法
      rdd => {
        //obj.asInstanceOf[C]类似java中类型转换(C)obj
        //TODO 针对每个 DStream，Spark 会根据其生成的 RDD 和分区来创建多个任务分配到各个节点上。
        //TODO 如果这个Dstream流中只有一个转换操作，永远不会遇到动作操作（action），那么这个转换算子中的代码只会在driver端执行，而且针对RDD生成的执行计划，永远不会被执行。
        //如果下方代码在executor端执行，那么offsetRanges可能会被多个 executor 端的任务更新，这可能会导致不一致的结果。
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd//原样返回，不对DStream流中的数据做任何处理
      }
    )

    //kafkaDStream.print(100)，打印报错
    //此处的ConsumerRecord不支持Serializable接口，如果需要获取DStream数据打印并分流传递，则需要转换数据结构。
    //5. 处理数据
    //5.1 转换数据结构
    val jsonObjDStream: DStream[JSONObject] = offsetRangesDStream.map(
      consumerRecord => {
        //获取ConsumerRecord中的value,value就是日志数据
        val log: String = consumerRecord.value()
        //转换成Json对象
        val jsonObj: JSONObject = JSON.parseObject(log)
        //返回
        jsonObj
      }
    )
    // jsonObjDStream.print(1000)//处理流数据打印出json数据

    //5.2 分流
    // 日志数据：
    //   页面访问数据
    //      公共字段
    //      页面数据
    //      曝光数据
    //      事件数据
    //      错误数据
    //   启动数据
    //      公共字段
    //      启动数据
    //      错误数据
    val DWD_PAGE_LOG_TOPIC : String = "DWD_PAGE_LOG_TOPIC_1018"  // 页面访问
    val DWD_PAGE_DISPLAY_TOPIC : String = "DWD_PAGE_DISPLAY_TOPIC_1018" //页面曝光
    val DWD_PAGE_ACTION_TOPIC : String = "DWD_PAGE_ACTION_TOPIC_1018" //页面事件
    val DWD_START_LOG_TOPIC : String = "DWD_START_LOG_TOPIC_1018" // 启动数据
    val DWD_ERROR_LOG_TOPIC : String = "DWD_ERROR_LOG_TOPIC_1018" // 错误数据
    //分流规则:
    // 错误数据: 不做任何的拆分， 只要包含错误字段，直接整条数据发送到对应的topic
    // 页面数据: 拆分成页面访问， 曝光， 事件 分别发送到对应的topic
    // 启动数据: 发动到对应的topic

    jsonObjDStream.foreachRDD(//foreachRDD(func)是sparkStreaming的OutputOperation算子。
                              // 最通用的输出方式，它将函数 func 应用于从流生成的每个 RDD。
                              // 但是foreachRDD并不会触发立即处理，必须在碰到sparkcore的foreach或者foreachPartition算子后，才会触发action动作。
      rdd => {
        /*TODO 介于foreachRDD和foreachPartition之间的代码是在 Driver 端运行的，因为 这块代码是对每个RDD进行操作，并没有具体到每个分区，所以不会在每个分区对应的节点上执行。*/
        rdd.foreachPartition(//foreachPartition是spark-core的算子，作用于每个【rdd分区】
          jsonObjIter => {//jsonObjIter是包含分区中所有数据的一个迭代器
            for (jsonObj <- jsonObjIter) {//所有JSON数据对象
              //分流过程
              //分流错误数据
              val errObj: JSONObject = jsonObj.getJSONObject("err")
              if(errObj != null){
                //将错误数据发送到 DWD_ERROR_LOG_TOPIC
                MyKafkaUtils.send(DWD_ERROR_LOG_TOPIC ,  jsonObj.toJSONString )
              }else{
                // 提取公共字段
                val commonObj: JSONObject = jsonObj.getJSONObject("common")
                val ar: String = commonObj.getString("ar")
                val uid: String = commonObj.getString("uid")
                val os: String = commonObj.getString("os")
                val ch: String = commonObj.getString("ch")
                val isNew: String = commonObj.getString("is_new")
                val md: String = commonObj.getString("md")
                val mid: String = commonObj.getString("mid")
                val vc: String = commonObj.getString("vc")
                val ba: String = commonObj.getString("ba")
                //提取时间戳
                val ts: Long = jsonObj.getLong("ts")
                // 页面数据
                val pageObj: JSONObject = jsonObj.getJSONObject("page")
                if(pageObj != null ){
                  //提取page字段
                  val pageId: String = pageObj.getString("page_id")
                  val pageItem: String = pageObj.getString("item")
                  val pageItemType: String = pageObj.getString("item_type")
                  val duringTime: Long = pageObj.getLong("during_time")
                  val lastPageId: String = pageObj.getString("last_page_id")
                  val sourceType: String = pageObj.getString("source_type")

                  //封装成PageLog
                  var pageLog =
                    PageLog(mid,uid,ar,ch,isNew,md,os,vc,ba,pageId,lastPageId,pageItem,pageItemType,duringTime,sourceType,ts)
                  //发送到DWD_PAGE_LOG_TOPIC
                  /*
                  * scala中，JSON.toJSONString(pageLog) 无法直接打印 pageLog 对象的原因是 fastjson 库并不支持 Scala 对象的序列化。
                  * fastjson 库是为 Java 设计的，因此它默认只能序列化 Java 对象。
                  * 如果你要序列化 Scala 对象，你需要使用 fastjson 库中的 Scala 模块，或者使用其它支持 Scala 序列化的 JSON 库。
                  * 在调用 JSON.toJSONString(pageLog, new SerializeConfig(true)) 时，传入的 SerializeConfig 对象会启用 Scala 模块，
                  * 基于字段 序列化该对象 并转换成JSON字符串*/
                  MyKafkaUtils.send(DWD_PAGE_LOG_TOPIC , JSON.toJSONString(pageLog , new SerializeConfig(true)))

                  //提取曝光数据
                  val displaysJsonArr: JSONArray = jsonObj.getJSONArray("displays")
                  if(displaysJsonArr != null && displaysJsonArr.size() > 0 ){
                    for(i <- 0 until displaysJsonArr.size()){
                      //循环拿到每个曝光
                      val displayObj: JSONObject = displaysJsonArr.getJSONObject(i)
                      //提取曝光字段
                      val displayType: String = displayObj.getString("display_type")
                      val displayItem: String = displayObj.getString("item")
                      val displayItemType: String = displayObj.getString("item_type")
                      val posId: String = displayObj.getString("pos_id")
                      val order: String = displayObj.getString("order")

                      //封装成PageDisplayLog
                      val pageDisplayLog =
                        PageDisplayLog(mid,uid,ar,ch,isNew,md,os,vc,ba,pageId,lastPageId,pageItem,pageItemType,duringTime,sourceType,displayType,displayItem,displayItemType,order,posId,ts)
                      // 写到 DWD_PAGE_DISPLAY_TOPIC
                      MyKafkaUtils.send(DWD_PAGE_DISPLAY_TOPIC , JSON.toJSONString(pageDisplayLog , new SerializeConfig(true)))
                    }
                  }
                  //提取事件数据
                  val actionJsonArr: JSONArray = jsonObj.getJSONArray("actions")
                  if(actionJsonArr != null && actionJsonArr.size() > 0 ){
                    for(i <- 0 until actionJsonArr.size()){
                      val actionObj: JSONObject = actionJsonArr.getJSONObject(i)
                      //提取字段
                      val actionId: String = actionObj.getString("action_id")
                      val actionItem: String = actionObj.getString("item")
                      val actionItemType: String = actionObj.getString("item_type")
                      val actionTs: Long = actionObj.getLong("ts")

                      //封装PageActionLog
                      var pageActionLog =
                        PageActionLog(mid,uid,ar,ch,isNew,md,os,vc,ba,pageId,lastPageId,pageItem,pageItemType,duringTime,sourceType,actionId,actionItem,actionItemType,actionTs,ts)
                      //写出到DWD_PAGE_ACTION_TOPIC
                      MyKafkaUtils.send(DWD_PAGE_ACTION_TOPIC , JSON.toJSONString(pageActionLog , new SerializeConfig(true)))
                    }
                  }
                }
                // 启动数据
                val startJsonObj: JSONObject = jsonObj.getJSONObject("start")
                if(startJsonObj != null ){
                  //提取字段
                  val entry: String = startJsonObj.getString("entry")
                  val loadingTime: Long = startJsonObj.getLong("loading_time")
                  val openAdId: String = startJsonObj.getString("open_ad_id")
                  val openAdMs: Long = startJsonObj.getLong("open_ad_ms")
                  val openAdSkipMs: Long = startJsonObj.getLong("open_ad_skip_ms")

                  //封装StartLog
                  var startLog =
                    StartLog(mid,uid,ar,ch,isNew,md,os,vc,ba,entry,openAdId,loadingTime,openAdMs,openAdSkipMs,ts)
                  //写出DWD_START_LOG_TOPIC
                  MyKafkaUtils.send(DWD_START_LOG_TOPIC , JSON.toJSONString(startLog ,new SerializeConfig(true)))

                }
              }
            }
            /**
             * spark代码位置3,foreachPartition里面，每批次的一个分区执行一次（executor端）
             * --------------------------------------------------------
            * KafkaProducer.flush()，强制将生产者缓存中的所有待发送记录立即发送到Kafka集群中
            * 当生产者发送消息时，它会缓存消息，并在后台定期发送批量数据。
            * 默认情况下，生产者在内部管理事务以确保发送数据的完整性。
            * 如果您需要确保生产者在程序退出前立即发送所有缓存数据，可以使用flush()方法。
            *--------------------------------------------------------
            * 另外，为什么flush操作要在rdd.foreachPartition里面执行？
            * 因为如果flush操作在jsonObjDStream.foreachRDD里面，rdd.foreachPartition外面: 此时刷写Kafka缓冲区 的操作 是在 Driver端执行，即一批次执行一次（周期性）。
            * 而分流操作是在executor端完成，发送分流数据的Kafka生产者对象也是在executor端创建并管理。所以是不能在driver端做刷写的，刷的不是同一个对象的缓冲区。
            * */
            MyKafkaUtils.flush()
          }
        )
        /**
         * spark代码位置2,一批次执行一次（driver端）
         * -------------------------------------------------------------
         * //jsonObjDStream.foreachRDD里面，rdd.forech外面: 先让数据存盘(写出数据)，后提交offset到redis，避免数据丢失 -->  Driver段执行，一批次(包括该批次的所有分区)执行一次（周期性）
         * //注：offsetRanges包含该批次的全部offset信息。我们在saveOffset方法中使用了redis的hash结构，可以存放下该批次的全部offset信息。
         * */
        MyOffsetsUtils.saveOffset(topicName,groupId,offsetRanges)

        /**
        spark代码位置4,每条数据执行一次
        -------------------------------------------------------------
        对于手动提交偏移量和生产者刷写缓冲区数据到磁盘，还有一种做法如下（使用rdd.foreach算子）：
        rdd.foreach(
          jsonObj => { //jsonObj是RDD中的每个对象
            //foreach里面:  提交offset。-->  executor执行, 每条数据执行一次.操作每条数据一定是在executor端执行，。
            //foreach里面:  刷写kafka缓冲区。--> executor执行, 每条数据执行一次.  相当于是同步发送消息。
            //时间效率由高到低：仅启动程序时执行一次（driver端） > 一批次执行一次（driver端） > 每批次的一个分区执行一次（executor端） > 每条数据执行一次（executor端）。
            //有时间效率更高的且可行的做法，所以不选用这种做法。
          }
        )
         */
      }
    )
    /**
     * spark代码位置1,仅启动程序时执行一次（driver端）
     * -------------------------------------------------------------------
     * foreachRDD外面:  提交offsets。 -->  Driver执行，每次启动程序执行一次。不可行，无法通过不断手动提交偏移量解决kafka精确一次消费问题。
     * foreachRDD外面:  刷写kafka缓冲区。-->  Driver执行，每次启动程序执行一次。不可行，分流是在executor端完成，不能在driver端做刷写，刷的不是同一个对象的缓冲区.
     * */
    ssc.start()
    // 等待终止
    ssc.awaitTermination()
    // --------------------------------------------------------------------
  }
}
