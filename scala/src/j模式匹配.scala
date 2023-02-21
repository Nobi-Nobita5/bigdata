/*match对应java里的switch，但是写在选择器表达式之后。即： 选择器 match {备选项}。
* 使用了case关键字的类定义就是样例类(case classes)，样例类是种特殊的类，可以用于模式匹配。*/
object j模式匹配 {
  def main(args: Array[String]) {
    val alice = new Person("Alice", 25)
    val bob = new Person("Bob", 32)
    val charlie = new Person("Charlie", 32)

    for (person <- List(alice, bob, charlie)) {
      person match {
        case Person("Alice", 25) => println("Hi Alice!")
        case Person("Bob", 32) => println("Hi Bob!")
        case Person(name, age) =>
          println("Age: " + age + " year, name: " + name + "?")
      }
    }
  }
  // 样例类
  case class Person(name: String, age: Int)
}
