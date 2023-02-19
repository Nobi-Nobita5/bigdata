package com.atguigu.gmall.realtime.util

import java.lang.reflect.{Field, Method, Modifier}

import com.atguigu.gmall.realtime.bean.{DauInfo, PageLog}

import scala.util.control.Breaks

/**
  * 实现对象属性拷贝.
  */
object MyBeanUtils {

  def main(args: Array[String]): Unit = {
    val pageLog: PageLog =
      PageLog("mid1001" , "uid101" , "prov101" , null ,null ,null ,null ,null ,null ,null ,null ,null ,null ,0L ,null ,123456)

    val dauInfo: DauInfo = new DauInfo()
    println("拷贝前: " + dauInfo)

    copyProperties(pageLog,dauInfo)

    println("拷贝后: " + dauInfo)

  }

  /**
    * 将srcObj中属性的值拷贝到destObj对应的属性上.
    */
  def copyProperties(srcObj : AnyRef , destObj: AnyRef): Unit ={
    if(srcObj == null || destObj == null ){
      return
    }
    //获取到srcObj中所有的属性
    val srcFields: Array[Field] = srcObj.getClass.getDeclaredFields

    //处理每个属性的拷贝
    for (srcField <- srcFields) {
      Breaks.breakable{ //Breaks.break()后，退出该大括号中的代码块
        //get / set
        // Scala会自动为类中的属性提供get、 set方法
        /*Java 和 Scala 中的 getter 和 setter 方法都是用来访问和修改类中的属性的，
        但是 Scala 的 getter 和 setter 方法更为简洁，因为它们可以通过属性定义自动生成。*/
        // get : fieldname()
        // set : fieldname_$eq(参数类型)

        //getMethodName
        var getMethodName : String = srcField.getName
        //setMethodName
        var setMethodName : String = srcField.getName+"_$eq"

        //从srcObj中获取get方法对象，
        val getMethod: Method = srcObj.getClass.getDeclaredMethod(getMethodName)
        //从destObj中获取set方法对象
        // String name;
        // getName()
        // setName(String name ){ this.name = name }
        val setMethod: Method =
        try{
          destObj.getClass.getDeclaredMethod(setMethodName, srcField.getType)
        }catch{
          // NoSuchMethodException
              // case ex : Exception 是模式匹配语法，用于匹配某个值是否满足 Exception 类型。
          case ex : Exception =>  Breaks.break()
        }

        //忽略val属性
        val destField: Field = destObj.getClass.getDeclaredField(srcField.getName)
        if(destField.getModifiers.equals(Modifier.FINAL)){
          Breaks.break()
        }
        //使用反射机制获取 srcObj 对象的属性值，并使用反射机制将这个属性值设置到 destObj 对象中。
        // invoke() ，它用于调用对象的方法。getMethod.invoke(srcObj) 表示调用 srcObj 对象的 getter 方法，获取对应的属性值。
        setMethod.invoke(destObj, getMethod.invoke(srcObj))
      }

    }

  }

}
