package dec05

import dec03.Main.sc
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main extends App {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("AdventOfCode")
  @transient lazy val sc: SparkContext = new SparkContext(conf)


  val rawData   = sc.textFile("src/main/resources/input_05.txt")

  def reactPolarity(t:List[Char],c:Char):List[Char] = {
    if(
      !t.isEmpty
        && ((c.isUpper &&  t.head.isLower && c.equals(t.head.toUpper))
        ||  (c.isLower && t.head.isUpper && c.equals(t.head.toLower)))
    ) {
      t.tail
    }else{
      c :: t
    }
  }


  val solutionA = rawData.map(s=> s.toCharArray).flatMap(c => c)
    .aggregate(List[Char]())( reactPolarity,(a,b) => a ++ b).size


  println(solutionA)

  sc.stop()

}
