package dec01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import utils.RddBottomHelper

object Main extends  App{

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("AdventOfCode")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  val rawData   = sc.textFile("src/main/resources/input_01.txt")

  val signedData = rawData.map(s => s.toInt )

  def sumAcc(acc:Int, value:Int):Int = {acc + value}

  val solutionA = signedData.aggregate(0)(sumAcc,sumAcc)

  println(solutionA)

  def findFirstRepetition(sum: Int, data: RDD[Int], found: Set[Int] = Set.empty): Int = {
    if(found.contains(sum))
      sum
    else
      findFirstRepetition(data.first(), sc.parallelize(RddBottomHelper.bottom(data,1)), found + sum)
  }

  val solutionB = findFirstRepetition(0,signedData)

  println(solutionB)

  sc.stop()

}
