/*
lambda表达式，也称为闭包。
依赖于定义在函数外部的一个或多个变量
闭包可以简单理解为是可以访问一个函数里面局部变量的另外一个函数
*/
object d闭包 {
  def main(args: Array[String]): Unit = {
    print(multiplier(3))
  }
  var factor = 3
  val multiplier = (i:Int) => i* factor
}
