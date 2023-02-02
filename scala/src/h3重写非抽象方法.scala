class Person {
  var name = ""
  //toString方法
  //重写一个非抽象方法必须使用override修饰符。
  override def toString = getClass.getName + "[name=" + name + "]"
}

class Employee extends Person {
  var salary = 0.0
  override def toString = super.toString + "[salary=" + salary + "]"
}

object Test extends App {
  val fred = new Employee
  fred.name = "Fred"
  fred.salary = 50000
  println(fred)
}