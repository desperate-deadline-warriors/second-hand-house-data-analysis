package com.itbys.spark_test

import com.mongodb.spark.MongoSpark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document


/**
  * Author xx
  * Date 2021/12/22
  * Desc
  */

case class DataModel(id:String, hu_xing:String, area :String, chao_xiang:String, other:String, lou_cen:String, lei_xin:String, dan_jia:String, zong_jia:String)

object EtlMongo {

  def main(args: Array[String]): Unit = {

    //定义环境、连接
    val conf: SparkConf = new SparkConf().setAppName(getClass.getName).setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf)
      .config("spark.mongodb.output.uri", "mongodb://hdp/test.demo01")
      .getOrCreate()

    //转化实体类
    val modelRDD: RDD[DataModel] = spark.sparkContext.textFile("input/")
      .filter(x => x.length > 0 && !x.contains("hu_xing") && x.split(",").length == 10)
      .map {
        x =>
          val strings: Array[String] = x.replaceAll("[\\s*$&#/\"'\\.:;?!\\[\\](){}<>~\\_]+","").split(",")
          DataModel(strings(0), strings(1), strings(2), strings(3),strings(4),strings(5),strings(6),strings(7)+strings(8),strings(9))
      }
    modelRDD.foreach(println(_))

    //转化document对象
    val docRDD: RDD[Document] = modelRDD.map(
      x => {
        val document = new Document()
        document.append("id", x.id).append("hu_xing", x.hu_xing).append("area", x.area).append("chao_xiang", x.chao_xiang).append("other", x.other)
          .append("lou_cen", x.lou_cen).append("lei_xin", x.lei_xin).append("dan_jia", x.dan_jia).append("zong_jia", x.zong_jia)
        document
      }
    )

    MongoSpark.save(docRDD)

  }

}
