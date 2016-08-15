package mainpage

import org.apache.spark.SparkContext

/**
  * Created by hzqianwei on 2016/8/3.
  *
  */

class MainPageNoteRelevance extends Serializable {

  /**
    * note relevant recommendations
    */
  def noteRelevant(sc:SparkContext, inputPath:Array[String], noteRelevantSavedPath:String): Unit = {

    //  心得表位置
    val notePath = inputPath(0)

    // 心得标签表位置
    val noteLabelPath = inputPath(1)

    // 心得质量表的位置
    val noteScorePath = inputPath(2)

    //1. get (user_id, label) pair
    val noteTable = sc.textFile(notePath)
    val noteLabelTable = sc.textFile(noteLabelPath)

    val userNote = noteTable.map(x => x.split("\t")).map(x => (x(1), x(0)))
    val noteUser = userNote.map(x => (x._2, x._1))
    val noteLabel = noteLabelTable.map(y => y.split("\t")).map(x => (x(1), x(2)))
    val labelNote = noteLabel.map(x => (x._2, x._1))

    // 将 心得-用户 和 心得-标签 关联,  得到用户标签
    val userLabel = noteUser.join(noteLabel).map(x => (x._2._1, x._2._2))
    val labelUser = userLabel.map(x => (x._2, x._1))

    // 2. 用户心得关联推荐结果    结构:(user_id, note_id)
    // 将上一步得到的 用户-标签   和标签-心得关联, 得到 用户-推荐心得, 这样关联里面肯定有用户已经看过的心得  所以subtract是去掉用户自己的心得 distinct 是用来去重
    //    这里没有考虑权重   后面修改
    val userNoteRecommend = labelUser.join(labelNote).map(x => (x._2._1, x._2._2)).subtract(userNote).distinct()

    // 3. 读取心得质量, 然后在关联的当中选取质量高的来排序, 暂时,没有数据, 下一次数据量里面数据全了再修改
    val noteScore = sc.textFile(noteScorePath)
    val userNoteRecommendScore = null

    val userNoteRecommendation = userNoteRecommend.groupByKey().map(x => x._1 + "\t" + x._2.mkString(","))
    userNoteRecommendation.foreach(println)

    userNoteRecommendation.coalesce(1).saveAsTextFile(noteRelevantSavedPath)
  }


  def listRelevant(sc:SparkContext, inputPath:Array[String]): Unit = {

  }

}
