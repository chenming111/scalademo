package com.adc.spark

import org.apache.spark.sql.SparkSession

object UntypedOperation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("UntypedOperation")
      .master("local")
      .config("spark.sql.warehouse.dir", "C:\\Users\\Administrator\\Desktop\\spark-warehouse")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._
    val employee = spark.read.json("C:\\Users\\Administrator\\Desktop\\employee.json")
    val department = spark.read.json("C:\\Users\\Administrator\\Desktop\\department.json")

  employee
      .where("age >20")
      .join(department,$"depId" === $"id")
      .groupBy(department("name"),employee("gender"))
      .agg(avg(employee("salary")))
      .show()

    employee.select($"name",$"depId",$"salary")
      .where("age>30")
      .show()






  }

}
