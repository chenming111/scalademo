package com.adc.spark

import org.apache.spark.sql.SparkSession

object Test {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession

      .builder()

      .master("local")

      .appName("HdfsTest")

      .getOrCreate()

    val filePath= "C:\\Users\\Administrator\\Desktop\\spark.txt"



    //dataset方式
    import spark.implicits._

    val dataset = spark.read.textFile(filePath)
      .flatMap(x => x.split(" "))
      .map(x => (x,1))
      .groupBy("_1")
      .count().show()




  }
}
