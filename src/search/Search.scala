package search

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hzqianwei on 2016/8/15.
  */
object Search {

  def main(args: Array[String]) {

    val indexWithIDFPath = "F:\\NetEase\\search_eng\\indexWithIDF"

    val conf = new SparkConf().setAppName("mainPage").setMaster("local")
    val sc = new SparkContext(conf)

    // 搜索关键字 : "云南白药牙膏留兰香型"
    val ta = "云南白药牙膏留兰香型"

    // 搜索关键字分词
    val taList = CreateIndex.segmenter(ta)
    val searchWordSet = taList.toSet
    taList.foreach(println)

    val information = sc.textFile("E:\\test_search_data", 4).map(x => x.split("\t")).map(x => (x(0), (x(2).replace("【京东超市】", ""), x(3))))
    val index = sc.textFile("E:\\index").map(x => (x.split("\t")(0), x.split(";")))

    // 查询词的数组
    val idArray = index.filter{
      x => isContainString(taList, x._1)
    }.flatMap(x => x._2).collect()

    // 找到倒排索引里面词对应的id
    // 并且形成元组   (doc_id, Array[(word, score)])
    val indexWithScore = sc.textFile(indexWithIDFPath)
      .map(_.split("\t"))
      .map{x => (x(0), x(1).split(";").map(y => (y.split(",")(0), y.split(",")(1))))}          //   word   doc_id,score;doc_id,score
      .filter(x => isContainString(taList, x._1))
      .flatMap{x => x._2.map(y => (y._1, (x._1, y._2)))}
      .groupByKey()
      .map(x => (x._1, x._2.toArray))

    // 计算相似度
    val simSearched = indexWithScore

    val searched = information.filter{
      x => isContainString(idArray, x._1)
    }.map(x => (sim(ta, x._2._1), (x._1, x._2._1, x._2._2)))

    searched.sortBy(x => x._1).foreach(println)

    sc.stop()
    System.exit(0)
    //.foreach(x => x.foreach(println))
  }

  def sim(q:String, str:String): Double = {
    val arr_1 = CreateIndex.segmenter(q)
    val arr_2 = CreateIndex.segmenter(str)
    val r = arr_1.flatMap(a1 => arr_2.map(a2 => (a1, a2))).distinct.count(x => x._1.equals(x._2))
    r.toDouble / arr_1.length.toDouble
  }

  // 带权重的相似度计算
  /**
    *
    * @param q  输入的查询词, 例如 云南白药
    * @param arr  文档内容, 例如: 云南白药牙膏
    */
  def simWithScore(q:String, arr:Array[(String, String)]): Unit ={
    val arr_1 = CreateIndex.segmenter(q).toSet
    var sim = 0.0
    for (x <- arr) {
      val word = x._1
      val score = x._2.toDouble
      if (arr_1.contains(word))
        sim = sim + score
    }

  }


  def isContainString(taList:Array[String], string: String): Boolean = {
    val tSet = taList.toSet[String]
    if (tSet.contains(string))
      true
    else
      false
  }
}
