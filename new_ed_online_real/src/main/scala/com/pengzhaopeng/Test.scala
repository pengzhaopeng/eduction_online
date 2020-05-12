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
    //    println(arr1.union(arr2).toList.distinct)
    //    println(arr1.toList)

    val arr = Array("0-1", "2-8", "22", "9-10", "11-15")
    for (i <- arr.indices) {
      var start = 0
      var end = 0
      try {
        start = arr(i).split("-")(0).toInt
        end = arr(i).split("-")(1).toInt
      } catch {
//        case e: Exception => throw new Exception("error msg:" + arr.mkString(","))
        case e: Exception => e.printStackTrace()
      }
    }
  }
}
