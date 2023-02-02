class c访问修饰符 {
  class inner{
      private def f(): Unit ={
      println("f")
    }
    class  InnerMost{
      f() //正确
    }
  }
  //(new inner).f() //错误
}
/*
(new Inner).f( ) 访问不合法是因为 f 在 Inner 中被声明为 private，而访问不在类 Inner 之内。

但在 InnerMost 里访问 f 就没有问题的，因为这个访问包含在 Inner 类之内。

Java 中允许这两种访问，因为它允许外部类访问内部类的私有成员。
*/