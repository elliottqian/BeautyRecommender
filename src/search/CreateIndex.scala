package search

import org.apache.spark.{SparkConf, SparkContext}
import org.apdplat.word.WordSegmenter
import org.apdplat.word.segmentation.{SegmentationAlgorithm, Word}
import java.lang.Math

/**
  * Created by hzqianwei on 2016/8/15.
  *
  */
object CreateIndex {

  /**
    * 主要建立 word doc_id,score;doc_id,score;doc_id,score......    的倒排索引表
    *
    * IDF值:log(文档总数量 / 出现该词的文档数量 + 1)
    * TF值: 词频
    *
    * 首先取出3元祖: (doc_id, doc_content, hot)
    * 对每个三元组用 getTupleArray 方法 得到 (词, (文档_id, 这个词在这个文档的权值))
    * 然后分组
    * 存储
    *
    * @param args
    */
  def main(args: Array[String]) {

    val dataPath = MyPath.dataPath
    val indexPath = "F:\\NetEase\\search_eng\\tf"
    val indexWithIDFPath = MyPath.indexWithIDFPath;
    val docContentPath = "F:\\NetEase\\search_eng\\docContent"

    val conf = new SparkConf().setAppName("mainPage").setMaster("local")
    val sc = new SparkContext(conf)

    // 读取文件
    val rdd = sc.textFile(dataPath, 4)

    // 获得三元组
    val information = rdd.map(x => x.split("\t")).map(x => (x(0), x(2).replace("【京东超市】", ""), x(3)))

    // 词频
    val tf = information.flatMap(x => segmenter(x._2)).map(x => (x, 1)).reduceByKey((x, y) => x + y)

    // 不带权值的索引表
    /**
      * val index = information.flatMap(x => segmenter(x._2).map(y => (y, x._1))).groupByKey().map(x => x._1 + "\t" +x._2.toArray.mkString(";"))
      * index.saveAsTextFile(indexPath)
      */

    // TF的map形式和文档数量
    val wordHash = tf.collect().toMap
    val docNum = information.count()

    // 获得 (词, (文档_id, 这个词在这个文档的权值)) 然后按照词分组
    val indexWithIDF = information.flatMap(x => getTupleArray(x._2, wordHash, docNum, x._1))
      .groupByKey()
      .map(x => x._1 + "\t" +x._2.toArray.mkString(";").replace("(", "").replace(")", ""))

    //存储
    indexWithIDF.coalesce(4).saveAsTextFile(indexWithIDFPath)


    // 获得 文档 内容 词 分数 热度 并且保存
    val docIndex = information.map{
      //  map{x => (x, Math.log(docNum / wordHash.get(x).get.toDouble))}  这个是计算idf的值
      x => (x._1, x._2, segmenter(x._2).map{x => (x, Math.log(docNum / wordHash.get(x).get.toDouble))}, x._3)
    }.map{ x =>
      x._1 + "\t" + x._2 + "\t" + x._3.mkString(";").replace("(", "").replace(")", "") + "\t" + x._4
    }
    docIndex.coalesce(4).saveAsTextFile(docContentPath)

    sc.stop()
    System.exit(0)
  }

  /**
    * 分词方法
    * @param s    出入的文档内容
    * @return     分词的词组
    */
  def segmenter(s:String): Array[String] = {
    val x = WordSegmenter.seg(s, SegmentationAlgorithm.MinimumMatching).toArray.map(x => x.toString)
    x
  }

  /**
    * 1. 分词
    * 2. 获得 (词, 词频) 的二元组
    * 3. 获得 (词, 文档_id, IDF) 的三元组
    *
    * @param doc      文档内容
    * @param map      word 的 tf键值对
    * @param doc_num  文档总数量
    * @param doc_id   文档id
    * @return         (词, (文档_id, 这个词在这个文档的权值))
    */
  def getTupleArray(doc:String, map:Map[String, Int], doc_num:Long, doc_id:String): Array[(String, (String ,Double))] = {
    // map.get(x)  就是取得对应文档的词的个数
    val wordArray = segmenter(doc).map(x => (x, map.get(x).get.toDouble)).map{
      x => (x._1, (doc_id, Math.log(doc_num / x._2)))
    }
    wordArray
  }

}
