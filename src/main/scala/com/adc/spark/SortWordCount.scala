package com.adc.spark

/**
  * 排序版wordcount
  */

import org.apache.spark.sql.SparkSession
object SortWordCount {
  def main(args: Array[String]): Unit = {

    val filePath= "C:\\Users\\Administrator\\Desktop\\spark.txt"

    val spark = SparkSession.builder()
      .appName("SortWordCount")
      .master("local")
      .getOrCreate()
    val lines = spark.sparkContext.textFile(filePath)
    val words = lines.flatMap{line => line.split(" ")}
    val wordCounts = words.map{word => (word,1)}.reduceByKey(_ + _)
    val countWord = wordCounts.map{word => (word._2,word._1)}
    val sortedCountWord = countWord.sortByKey(false)
    val sortedWordCount = sortedCountWord.map{word => (word._2,word._1)}
    sortedWordCount.saveAsTextFile("C:\\Users\\Administrator\\Desktop\\wc.txt")
     sortedWordCount.foreach(s => {
       println("word \""+s._1+ "\" appears "+s._2+" times.")
     })

    spark.stop()
  }

}
