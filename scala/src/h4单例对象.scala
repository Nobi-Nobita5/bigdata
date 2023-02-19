import java.io._

//scala单例模式，用关键字object实现

class Point(val xc: Int, val yc: Int) {//1.类名定义时有参数
  //2.没有声明有参数的构造函数
  var x: Int = xc
  var y: Int = yc
  def move(dx: Int, dy: Int) {
    x = x + dx
    y = y + dy
  }
}

object Test1 {
  def main(args: Array[String]) {
    val point = new Point(10, 20)//3.创建对象时可以传递参数
    printPoint

    def printPoint{
      println ("x 的坐标点 : " + point.x);
      println ("y 的坐标点 : " + point.y);
    }
  }
}
