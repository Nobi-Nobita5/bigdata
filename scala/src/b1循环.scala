class b1循环 {
  def main(args: Array[String]): Unit = {
    var a = 0;
    val numList = List(1,2,3,4,5,6,7,8,9,10);

    /*
    for循环
    a <- numList，把集合的值赋给a
    a <- i to j，i到j的值赋给a
    if 是过滤条件
    yield，将for循环的值作为一个变量存储
     */
    var retVal = for{ a <- numList
                      if a != 3; if a < 8
                      }yield a

    // 输出返回值
    for( a <- retVal){
      println( "Value of a: " + a )
    }
  }
}
