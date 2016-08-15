package search

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hzqianwei on 2016/8/15.
  */
object Search {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("mainPage").setMaster("local")
    val sc = new SparkContext(conf)

    val ta = "云南白药牙膏留兰香型"
    val taList = SearchMain.segmenter(ta)
    taList.foreach(println)

    val information = sc.textFile("E:\\test_search_data", 2).map(x => x.split("\t")).map(x => (x(0), (x(2).replace("【京东超市】", ""), x(3))))
    val index = sc.textFile("E:\\index").map(x => (x.split("\t")(0), x.split(";")))

    val idArray = index.filter{
      x => isContainString(taList, x._1)
    }.flatMap(x => x._2).collect()

    val searched = information.filter{
      x => isContainString(idArray, x._1)
    }.map(x => (sim(ta, x._2._1), (x._1, x._2._1, x._2._2)))

    searched.sortBy(x => x._1).foreach(println)

    sc.stop()
    System.exit(0)
    //.foreach(x => x.foreach(println))
  }

  def sim(q:String, str:String): Double = {
    val arr_1 = SearchMain.segmenter(q)
    val arr_2 = SearchMain.segmenter(str)
    val r = arr_1.flatMap(a1 => arr_2.map(a2 => (a1, a2))).distinct.count(x => x._1.equals(x._2))
    r.toDouble / arr_1.length.toDouble
  }


  def isContainString(taList:Array[String], string: String): Boolean = {
    val tSet = taList.toSet[String]
    if (tSet.contains(string))
      true
    else
      false
  }
}
