object b方法与函数 {

  /*Scala 方法声明格式如下：
    def functionName ([参数列表]) : [return type]
    如果你不写等于号和方法主体，那么方法会被隐式声明为抽象(abstract)，包含它的类型于是也是一个抽象类型。
*/
  def main(args: Array[String]): Unit = {
    val res = addInt(1, 2)
    println(res)
  }
  def addInt(a:Int,b:Int) : Int = {
    var sum:Int = 0
    sum = a + b
    return sum
  }
}
