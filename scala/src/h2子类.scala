/*
Scala继承一个基类跟Java很相似, 但我们需要注意以下几点：

1、在子类中重写超类的抽象方法时，你不需要使用override关键字。
2、重写一个非抽象方法必须使用override修饰符。
3、只有主构造函数才可以往基类的构造函数里写参数。
*/


class h2子类 (override val xc: Int, override val yc: Int,
            val zc :Int) extends h1父类(xc, yc){
  var z: Int = zc

  def move(dx: Int, dy: Int, dz: Int) {
    x = x + dx
    y = y + dy
    z = z + dz
    println ("x 的坐标点 : " + x);
    println ("y 的坐标点 : " + y);
    println ("z 的坐标点 : " + z);
  }
}
