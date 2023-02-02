import java.io._

import scala.io.{Source, StdIn}

object l文件IO {
  def main(args: Array[String]) {
    //当前目录下生成文件，并写入
    val writer = new PrintWriter(new File("test.txt" ))

    writer.write("菜鸟教程")
    writer.close()

    //从屏幕上读取
    print("请输入菜鸟教程官网 : " )
    val line = StdIn.readLine()

    println("谢谢，你输入的是: " + line)

    //从文件上读取
    println("文件内容为:" )

    Source.fromFile("test.txt" ).foreach{
      print
    }
  }
}
