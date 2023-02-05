import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
 * @Author: Xionghx
 * @Date: 2023/02/04/10:23
 * @Version: 1.0
 */

object WordCount {
  def main(args: Array[String]): Unit = {
    new SparkContext(new SparkConf()
      .setAppName("WC")
      .setMaster("local"))
      .textFile("D:/scalatest.txt")
      .flatMap(line=>{line.split(" ")})
      .map(c=>(new Tuple2(c,1)))
      .reduceByKey((v1:Int,V2:Int) =>(v1+V2)).foreach(tp=>(println(tp)))
  }
}

