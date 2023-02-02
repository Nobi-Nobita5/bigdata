object f集合 {
  def main(args: Array[String]): Unit = {
    // 定义整型 List
    val a = List(1,2,3,4)

    // 定义 Set
    val b = Set(1,3,5,7)

    // 定义 Map
    val c = Map("one" -> 1, "two" -> 2, "three" -> 3)

    // 创建两个不同类型元素的元组
    val d = (10, "Runoob")

    // 定义 Option ,表示可能有包含值的容器，也可能不包含值
    val e:Option[Int] = Some(5)
  }
}
