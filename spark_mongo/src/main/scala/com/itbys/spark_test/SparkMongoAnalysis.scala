package com.itbys.spark_test

import com.mongodb.spark.MongoSpark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bson.Document

/**
  * Author xx
  * Date 2021/12/22
  * Desc
  */

object SparkMongoAnalysis {

  def main(args: Array[String]): Unit = {

    //定义环境、连接
    val spark: SparkSession = SparkSession.builder().appName(getClass.getName).master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://hdp/test.demo01")
      .config("spark.mongodb.output.uri", "mongodb://hdp/test.demo02")
      .getOrCreate()
    import spark.implicits._

    //加载数据、创建视图
    val df: DataFrame = MongoSpark.load(spark)
    df.createOrReplaceTempView("house_info")
    df.show()

    //分析：热门户型
    val resDf: DataFrame = spark.sql(
      """
        |select hu_xing, count(*) cnt
        |from house_info
        |group by hu_xing
        |order by cnt desc
        |limit 5
      """.stripMargin)

    val resultRDD: RDD[(String, String)] = resDf.as[(String,String)].rdd

    //转化document对象
    val docRDD: RDD[Document] = resultRDD.map(
      x => {
        val document = new Document()
        document.append("unit_type", x._1).append("cnt", x._2)
        document
      }
    )

    //写入mongo
    MongoSpark.save(docRDD)

  }


}
