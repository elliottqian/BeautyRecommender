package mainpage

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hzqianwei on 2016/8/5.
  */
object Test extends App{
  override def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("mainPage")//.setMaster("local")
    val sc = new SparkContext(conf)

    val x = sc.textFile("/user/ndir/beauty_recommend/Beauty_Note")
    val importInfo = x.map( x => x.split("\t")).map(result => (result(0), result(4), result(9), result(12)))
    importInfo.saveAsTextFile("/user/ndir/beauty_recommend/test")

    sc.stop()
  }
}
