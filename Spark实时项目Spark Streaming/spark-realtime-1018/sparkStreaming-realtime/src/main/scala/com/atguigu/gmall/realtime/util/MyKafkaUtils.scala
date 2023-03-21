package com.atguigu.gmall.realtime.util

import java.util

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable

/**
  * Kafka工具类， 用于生产数据和消费数据
  */
object MyKafkaUtils {

  /**
    * 消费者配置
    *
    * ConsumerConfig
    */
    //mutable集合与immutable集合的区别也很好理解，mutable内容可以修改，而immutable集合初始化之后，内容是不能修改的
  private val consumerConfigs: mutable.Map[String, Object] = mutable.Map[String,Object](
    // kafka集群位置
    //ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
    //ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> MyPropsUtils("kafka.bootstrap-servers"),
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS),

    // kv反序列化器
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    // groupId
    // offset提交  自动 手动
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
    //自动提交的时间间隔
    //ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG
    // offset重置  "latest"  "earliest"
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest"
    // .....
  )

  /**
    * 基于SparkStreaming消费 ,获取到KafkaDStream ,方便后续使用spark处理数据， 使用默认的offset
    */
  def getKafkaDStream(ssc : StreamingContext , topic: String  , groupId:String  ) ={
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG , groupId)
    /**
     * LocationStrategies.PreferBrokers() 是当在你kafka集群和spark集群部署节点存在同一服务器上的时候，任务的 executor 会优先运行该节点上，节省了网络IO，缺点是容易造成资源热点问题，不推荐使用。
     * LocationStrategies.PreferConsistent() 大多数情况下使用，不考虑kafka集群所在的影响，均匀的分配分区所有 executor 在spark集群上。
     * 新的Kafka使用者API将预先获取消息到缓冲区。因此，出于性能原因，Spark集成将缓存的消费者保留在执行程序上（而不是为每个批处理重新创建它们），并且更喜欢在具有适当使用者的主机位置上安排分区，这一点很重要。
     * 在大多数情况下，您应该使用LocationStrategies.PreferConsistent，如上所示。这将在可用执行程序之间均匀分配分区。
     * 如果您的执行程序与Kafka代理在同一主机上，请使用PreferBrokers，它更愿意为该分区安排Kafka领导者的分区。
     */
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs))
    kafkaDStream
  }

  /**
    * 基于SparkStreaming消费 ,获取到KafkaDStream ,方便后续使用spark处理数据， 使用指定的offset
    */
  def getKafkaDStream(ssc : StreamingContext , topic: String  , groupId:String ,  offsets: Map[TopicPartition, Long]  ) ={
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG , groupId)

      //ConsumerRecord[String, String]，代表消费到的数据[K,V]类型是[String, String],调用ConsumerStrategies.Subscribe方法时即可指定泛型。
      // 不指定的话ConsumerRecord无法自动识别到消费的数据类型，会自动接收成ConsumerRecord[Nothing, Nothing]
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs , offsets))//Array(topic)，消费者组可以基于多个topic进行消费
    kafkaDStream
  }

  /**
    * 生产者对象
    */
  val producer : KafkaProducer[String,String] = createProducer()

  /**
    * 创建生产者对象
    */
  def createProducer():KafkaProducer[String,String] = {
    val producerConfigs: util.HashMap[String, AnyRef] = new util.HashMap[String,AnyRef]
    //生产者配置类 ProducerConfig
    //kafka集群位置
    //producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092,hadoop104:9092")
    //producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,MyPropsUtils("kafka.bootstrap-servers"))
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS))
    //kv序列化器
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , "org.apache.kafka.common.serialization.StringSerializer")
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , "org.apache.kafka.common.serialization.StringSerializer")
    //acks
    producerConfigs.put(ProducerConfig.ACKS_CONFIG , "all")
    //batch.size  16kb
    //linger.ms   0
    //retries
    //幂等配置
    /*"enable.idempotence"是Kafka生产者的一个配置参数，它控制生产者是否启用幂等性。
幂等性是指多次执行同一操作对系统状态的影响是一致的。在Kafka中，幂等性通常指生产者发送的消息只被接收一次，即使生产者在发送消息时出现故障。
如果"enable.idempotence"参数设置为"true"，则Kafka生产者启用幂等性，并使用一些技术（例如发送消息的唯一标识符和重试策略）来确保消息不会重复发送。
如果"enable.idempotence"参数设置为"false"，则Kafka生产者不启用幂等性，并且在出现故障时可能会重复发送消息。
启用幂等性的好处是可以确保消息不会重复发送，从而保证消息的一致性和可靠性。
但是，它也带来了一些开销，因为生产者需要在消息发送之前确定消息的唯一标识符，并在消息发送失败时执行重试操作。
因此，在选择是否启用幂等性时，需要考虑应用程序的性能和可靠性要求。
*/
    producerConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG , "true")

    val producer: KafkaProducer[String, String] = new KafkaProducer[String,String](producerConfigs)
    producer
  }

  /**
    * 生产（按照默认的黏性分区策略）
    */
  def send(topic : String  , msg : String ):Unit = {
    producer.send(new ProducerRecord[String,String](topic , msg ))
  }

  /**
    * 生产（按照key进行分区）
    */
  def send(topic : String  , key : String ,  msg : String ):Unit = {
    producer.send(new ProducerRecord[String,String](topic , key ,  msg ))
  }

  /**
    * 关闭生产者对象
    */
  def close():Unit = {
    if(producer != null ) producer.close()
  }

  /**
    * 刷写 ，将缓冲区的数据刷写到磁盘
    *
    */
  def flush(): Unit ={
    producer.flush()
  }
}
