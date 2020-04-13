package com.pengzhaopeng

/**
  * description 
  * author 鹏鹏鹏先森
  * date 2020/4/14 0:54
  * Version 1.0
  */
object Test {
  def main(args: Array[String]): Unit = {
    val arr1 = Array("0", "4", "8")
    val arr2 = Array("0", "1", "2", "3", "4", "9")
    println(arr1.union(arr2).toList.distinct)
  }
}
