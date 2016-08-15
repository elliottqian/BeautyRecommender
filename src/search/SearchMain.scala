package search

import org.apache.spark.{SparkConf, SparkContext}
import org.apdplat.word.WordSegmenter
import org.apdplat.word.segmentation.Word

/**
  * Created by hzqianwei on 2016/8/15.
  *
  */
object SearchMain {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("mainPage").setMaster("local")
    val sc = new SparkContext(conf)


    val rdd = sc.textFile("E:\\test_search_data", 4)
    val information = rdd.map(x => x.split("\t")).map(x => (x(0), x(2).replace("【京东超市】", ""), x(3)))
    information.foreach(println)

    val index = information.flatMap(x => segmenter(x._2).map(y => (y, x._1))).groupByKey().map(x => x._1 + "\t" +x._2.toArray.mkString(";"))
    index.coalesce(2).saveAsTextFile("E:\\index")



//    val idRdd = information.filter{
//      x => isContainString(taList, x._1)
//    }.flatMap(x => x._2.toArray)//.foreach(x => x.foreach(println))
      //.foreach(println)
    //segmenter("你好 word 我不好").foreach(println)
    sc.stop()
    System.exit(0)
  }

  def segmenter(s:String): Array[String] = {
    val x = WordSegmenter.seg(s).toArray.map(x => x.toString)
    x
  }
}
