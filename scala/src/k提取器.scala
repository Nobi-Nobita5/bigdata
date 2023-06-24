object k提取器 {
  def main(args: Array[String]) {
    /*在我们实例化一个类的时，可以带上0个或者多个的参数，【编译器在实例化的时会调用 apply 方法】。我们可以在类和对象中都定义 apply 方法。*/
    val x = k提取器(5)
    println(x)

    x match
    {
      case k提取器(num) => println(x + " 是 " + num + " 的两倍！")
        //unapply 用于提取我们指定查找的值，它与 apply 的操作相反。当我们在提取器对象中使用match语句时，unapply将自动执行
      //unapply 被调用
      case _ => println("无法计算")
    }

  }
  def apply(x: Int) = x*2
  def unapply(z: Int): Option[Int] = if (z%2==0) Some(z/2) else None
}
