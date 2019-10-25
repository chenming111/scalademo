package com.adc.spark

import java.util.Properties

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
  * 从数据库spark_project表test_task读取task_id对应的taskparam字段
  * 进行wordcount分析后存入另一个表test_task_result
  *
  */
object ReadDataFromMysql {
  def main(args: Array[String]): Unit = {
    // 设置程序入口参数
    val task_id =5
    val spark = SparkSession
      .builder()
      .appName("ActionOperation")
      .master("local")
      .config("spark.sql.warehouse.dir", "C:\\Users\\Administrator\\Desktop\\spark-warehouse")
      .getOrCreate()

    // 创建数据库链接，加载数据，默认转为df
    val jdbcDF = spark.read.format("jdbc")
      .options(Map("url" -> "jdbc:mysql://60.247.58.117:50012/spark_project",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "test_task",
        "user" -> "root",
        "password" -> "Catarc@@123")).load()
    // df 注册为表，方便sql操作
    jdbcDF.createOrReplaceTempView("test_task")
    val taskParamDf = spark.sql("select task_param from test_task where task_id ="+ task_id)
    //引入隐式转换
    import spark.implicits._
    //wordcount,按出现次数降序排序，结果为rdd，格式为（word,count)
    val data =taskParamDf.rdd.flatMap(x => {
      x.getString(0).split(",")
    }).map(x => (x,1)).reduceByKey((x,y) => x+y).sortBy(tuple => tuple._2,false)

    //加入task_id字段,格式为(task_id,word,count),转为dataframe，
    val resultDf = data.map(word =>(task_id,word._1,word._2)).toDF()

   val schema = StructType(
     List(
       StructField("task_id", IntegerType, true),
       StructField("word", StringType, true),
       StructField("count", IntegerType, true)
     )
   )
    //与数据库字段映射，创建结果dataframe
    val result = spark.createDataFrame(resultDf.rdd, schema)
  //创建Properties存储数据库相关属性
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "Catarc@@123")
    //将数据追加到数据库
    result.write.mode("append").jdbc("jdbc:mysql://60.247.58.117:50012/spark_project", "test_task_result", prop)

  }
}
