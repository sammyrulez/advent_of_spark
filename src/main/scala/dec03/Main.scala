package dec03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import utils.RddBottomHelper

object Main extends  App{

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("AdventOfCode")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  val parsingRegex = "#([0-9]+) @ ([0-9]+),([0-9]+): ([0-9]+)x([0-9]+)".r

  case class WorkArea(elvenId: Int, x: Int, y: Int, width: Int, height: Int) extends Serializable

  val rawData   = sc.textFile("src/main/resources/input_03.txt")

  val workMap = rawData.map{
    case parsingRegex(id, x, y, w, h) => WorkArea(id.toInt, x.toInt, y.toInt, w.toInt, h.toInt)
  }

  val workMapPolarCoordinatesBySquareInch = for {
    workingArea <- workMap
    x <- 0 until workingArea.width
    y <- 0 until workingArea.height
  } yield (x + workingArea.x, y + workingArea.y)

  val solutionA = workMapPolarCoordinatesBySquareInch.groupBy(s => (s._1, s._2)).filter(_._2.size > 1).count()

  println(solutionA)


  sc.stop()

}
