package mainpage

import java.io.{File, PrintWriter}
import java.lang.Math._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.DataUtils

/**
  * Created by hzqianwei on 2016/8/3.
  */

class MainPageQuality extends Serializable{

  def calculateNoteQuality(sc:SparkContext, inputFile:String, savePath:String): RDD[(String, Double, Int)] = {

    val note: RDD[String] = sc.textFile(inputFile)

    // 1.id  2.content  3.score  4.update time
    val importInfo = note.map( x => x.split("\t")).map(result => (result(0), result(4), result(9), result(12)))

    // function 函数是计算质量函数
    val importance = importInfo.map(r => (r._1, function(r._2, r._3, r._4)))

    for (t <- importance.collect) {
      System.out.println(t)
    }

    importance.map(x => x._1.toString + "\t" + x._2.toString).coalesce(1).saveAsTextFile(savePath)

    importance.map(x => (x._1, x._2, 1))
  }


  /**
    * 计算心得质量前100
    */
  def calculateQualityTop(sc:SparkContext, importance:RDD[(String, Double, Int)], file_path:String): (String, Array[String]) ={
    val topN = 10
    val ascending = true
    // 对每个partition 取前N个  然后再把这N个排序
    val topImportant = importance.mapPartitions{
      x => topNSort(x.toArray, topN)
    }.sortBy(x => x._2, !ascending).take(topN)


    topImportant.foreach(println)

    val firstLineSavedFormat = "default" + "\t" + topImportant.map(x => x._1 + ":" + x._2.toString + ":" + x._3.toString).mkString(",")

    sc.parallelize(Array(firstLineSavedFormat)).coalesce(1).saveAsTextFile(file_path)

    (firstLineSavedFormat, null)
  }

  /**
    * 取得数组前N个数字
    * 冒泡排序
    */
  def topNSort(ar:Array[(String, Double, Int)], topN:Int): Iterator[(String, Double, Int)] ={
    val length = ar.length
    // 冒泡排序, 排序的是第二个关键字
    for(i <- 0 until topN - 1) {
      System.out.print(i)
      for (j <- i until length - 2) {
        if (ar(j)._2 > ar(j + 1)._2) {
          val temp = ar(j + 1)
          ar(j + 1) = ar(j)
          ar(j) = temp
        }
      }
    }//for
    //返回topN
    ar.reverse.take(topN).toIterator
  }

  /**
    * 心得权重函数
    * calculate quality of Note,
    * No the factor is content's length, score and time
    */
  def function(content:String,  score:String,  update_time:String): Double ={
    // 1. time factor of importance
    val timeFactor = 1

    // 2. length importance
    val contentImportance: Double = log(content.length) / 5.toDouble

    // 3. score importance
    var scoreImportance: Double = 0.0
    try {
      scoreImportance = Integer.getInteger(score).toDouble
    } catch { case e: Exception =>  }

    // 权重 = [ 分数 + log(心得长度) ] * 时间衰减因子
    (scoreImportance + contentImportance) * timeFactor
  }


  /**
    * 清单先要去关联清单媒体表, 然后join, 找到清单id对应的图片, 心得等, 然后根据这些计算清单质量
    *
    */
  def calculateListQuality(sc:SparkContext, Beauty_Repo:String, Beauty_RepoMedia:String, listQualityOut:String): RDD[(String, Double, Int)] = {

    val beautyRepo = sc.textFile(Beauty_Repo)
    val beautyRepoMedia = sc.textFile(Beauty_RepoMedia)

    beautyRepo.foreach(println)

    // 1.note_id  2.user_id 3.type 4.update time
    val beautyRepoInfo = beautyRepo.map(x => x.split("\t")).map(r => (r(0), (r(1), r(2), r(8))))

    val beautyRepoMediaInfo = beautyRepoMedia
      .map(x => x.split("\\t")).map(r => (r(1), calculateListSelectInfo(r)))
      .filter(x => !"-1".equals(x._2._1))
      .groupByKey()

    beautyRepoMediaInfo.foreach(println)

    val result = beautyRepoInfo.leftOuterJoin(beautyRepoMediaInfo)
    beautyRepoInfo.foreach(println)
    result.foreach(x => println(x))


    val importance = result.map(x => (x._1, imp(x._2._2), 1))
    importance.foreach(x => println(x))

    importance.map(x => x._1.toString + "\t" + x._2.toString).coalesce(1).saveAsTextFile(listQualityOut)


    importance.map(x => (x._1, x._2.toDouble, x._3))

  }

  def imp(datum:Option[Iterable[(String,String)]]): Int = {
    if (datum.isEmpty)
      0
    else
      datum.get.toArray.length
  }

  def calculateListSelectInfo(r:Array[String]): (String, String) ={
    val _type = r(2)
    println(r(2))
    _type match {
      case "1"   => (r(2), r(3))
      case "2"   => (r(2), r(4))
      case "3"    => (r(2), r(5))
      case _ => ("-1", "-2")
    }
  }

  /**
    * 保存到本地的方法
    * t._1   topN排序
    * t._2   推荐列表
    */
  def writeToLocal(path: String, t:(String, Array[String])): Unit = {
    val writer = new PrintWriter(new File(path))

    try{
      writer.println(t._1)
      // 后续添加其他东西
    }catch{
      case e: Exception => e.printStackTrace()
    }finally {
      writer.close()
    }
  }
}
