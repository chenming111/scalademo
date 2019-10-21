package com.adc.spark

/**
  *
  * 二次排序主函数
  *
  */

import org.apache.spark.sql.SparkSession
object SecondSort {

  def main(args: Array[String]): Unit = {

    val filepath = "C:\\Users\\Administrator\\Desktop\\sort.txt"
    val spark = SparkSession.builder()
      .appName("SecondSort")
      .master("local")
      .getOrCreate()
    val lines = spark.sparkContext.textFile(filepath)
    val pairs = lines.map{line => (
      new SecondSortKey(line.split(" ")(0).toInt,line.split(" ")(1).toInt),line

    )}
    val sortedPairs = pairs.sortByKey()

    val sortedLines = sortedPairs.map(pairs => pairs._2)
    sortedLines.foreach(s => println(s))

    spark.stop()


  }

}
