import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * @Author: Xionghx
 * @Date: 2023/02/03/20:07
 * @Version: 1.0
 */
object Test {
  def main(args: Array[String]): Unit = {
    //用sparkSession创建DataFramem  7
    val spark = SparkSession
      .builder()
      .master("local")//告诉spark,运行方式，本地单线程运行
      .appName("AuthorsAges")
      .getOrCreate()
    //创建包含名字和年龄的DataFrame
    val dataDF = spark.createDataFrame(Seq(("Brooke",20),("Lisa",25))).toDF("name","age")
    //以name聚合，求平均年龄
    val avgDF =  dataDF.groupBy("name").agg(avg("age"))
    //展示结果，动作
    avgDF.show()
  }
}
