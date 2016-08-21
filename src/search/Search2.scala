package search

import org.apache.spark.{SparkConf, SparkContext}
import java.lang.Math

import org.apache.spark.rdd.RDD

/**
  * Created by QianWei on 2016/8/21.
  */
object Search2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("mainPage").setMaster("local").set("log4j.logger.org.apache.spark", "WARN")
    val sc = new SparkContext(conf)

    val searchWord = "黑人超白牙膏竹炭深洁"
    val searchWordSet = CreateIndex.segmenter(searchWord).toSet[String]
    searchWordSet.foreach(println)

    // id  商品名称  热度
    val information = sc.textFile(MyPath.docContentPath, 4)
      .map(x => x.split("\t"))
      .map(x => (x(0), (x(2), x(3), x(1))))

    val indexIdf = sc.textFile(MyPath.indexWithIDFPath, 4)
      .map(x => x.split("\t"))
      .map(x => (x(0), x(1).split(";")))

    cal(searchWordSet, indexIdf, information)

  }

  def cal(searchWordSet:Set[String], indexIdf:RDD[(String, Array[String])],
          information:RDD[(String, (String, String, String))]): Int ={
    val relativeDocIdSet =  indexIdf.filter{
      x => isContainString(searchWordSet, x._1)
    }.flatMap{
      x => x._2.map(x => x.split(",")(0))
    }.collect().toSet[String]

    val relativeDocInfo = information.filter(x => isContainString(relativeDocIdSet, x._1))

    val  similarity = relativeDocInfo.map{ doc =>
      (doc._1, calSimilarity(searchWordSet, doc._2._1), doc._2._2, doc._2._3)
    }.sortBy(x => x._2, false).take(5)

    similarity.foreach(println)

    1
  }

  def calSimilarity(searchWordSet:Set[String], simString:String): Double ={
    // 皓,2.8457396038753964;乐,2.440274495767232
    val l = simString.split(";").map(x => (x.split(",")(0), x.split(",")(1))).toMap[String, String]

    val a = Math.sqrt(searchWordSet.size)
    var bb = 0.0
    var sim = 0.0

    for (key <- l.keys) {
      bb = bb + l.get(key).get.toDouble * l.get(key).get.toDouble
      if (searchWordSet.contains(key)) {
        sim += l.get(key).get.toDouble
      }
    }
    val b = Math.sqrt(bb)
    val r = sim / a / b
    r
  }

  def isContainString(taList:Set[String], string: String): Boolean = {
    val tSet = taList
    if (tSet.contains(string))
      true
    else
      false
  }
}
