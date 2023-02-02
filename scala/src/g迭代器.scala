object g迭代器 {
  def main(args: Array[String]): Unit = {
    //迭代器不是一个集合，是用于访问集合的一种方法
    //最简单的方法是使用while循环
    val it = Iterator("Baidu", "Google", "Runoob", "Taobao")

    while (it.hasNext){
      println(it.next())
    }

    //迭代器成员方法
    val ita = Iterator(20,40,2,50,69, 90)
    val itb = Iterator(20,40,2,50,69, 90)

    println("最大元素是：" + ita.max )
    println(itb.length)
  }
}
