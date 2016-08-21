package search

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by QianWei on 2016/8/21.
  */
object Search3 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("mainPage").setMaster("local").set("log4j.logger.org.apache.spark", "WARN")
    val sc = new SparkContext(conf)

    val searchRDD = sc.parallelize(Array("云南白药牙膏留兰香型", "黑人牙膏")).map(x => CreateIndex.segmenter(x).toSet[String])


  }
}
