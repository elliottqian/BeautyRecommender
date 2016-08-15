package mainpage

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import utils.DataUtils
//import utils.DataUtils

/**
  * Created by hzqianwei on 2016/8/5.
  */
object MainPageEntry extends App{

  /**
    * 算法标记
    * 1, 表示基本关联得出的分数
    */
  val BaseInfoCal = 1

  override def main(args: Array[String]) {

    val Beauty_Note = args(0)
    val Beauty_Repo = args(1)
    val Beauty_RepoMedia = args(2)
    val Beauty_NoteLabel = args(3)

    //清单和心得保存地址
    val noteQualitySavedPath = args(4)
    val listQualitySavedPath = args(5)

    // 心得关联推荐保存地址
    val noteRelevantSavedPath = args(6)
    // 清单关联推荐保存地址
    val listRelevantSavedPath = args(7)


    val conf = new SparkConf().setAppName("mainPage")//.setMaster("local")
    val sc = new SparkContext(conf)

    val mpq = new MainPageQuality()


    // 计算心得质量, 和top排序
    val noteImportance:RDD[(String, Double, Int)] = mpq.calculateNoteQuality(sc, Beauty_Note, noteQualitySavedPath)
    val noteTop = mpq.calculateQualityTop(sc, noteImportance, "/user/ndir/beauty_recommend/topN/test_Note_RecommendByUser." + DataUtils.getTodayString)
    //mpq.writeToLocal("/data/beauty_recommend/test_Note_RecommendByUser.", noteTop)


    // 计算清单质量 和 排序
    val listImportance:RDD[(String, Double, Int)] = mpq.calculateListQuality(sc, Beauty_Repo, Beauty_RepoMedia, listQualitySavedPath)
    val listTop = mpq.calculateQualityTop(sc, listImportance, "/user/ndir/beauty_recommend/topN/test_Repo_RecommendByUser." + DataUtils.getTodayString)
//    writeToLocal("/data/beauty_recommend/output_data/test_Repo_RecommendByUser." + DataUtils.getTodayString, listTop)

    // 计算心得关联  id  心得id列表
//    val mpnr= new MainPageNoteRelevance()
//    mpnr.noteRelevant(sc, Array(Beauty_Note, Beauty_NoteLabel, noteQualitySavedPath), noteRelevantSavedPath)

    sc.stop()


  }



}
