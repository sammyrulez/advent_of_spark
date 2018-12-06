package dec05

import dec03.Main.sc
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main extends App {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("AdventOfCode")
  @transient lazy val sc: SparkContext = new SparkContext(conf)


  val rawData   = sc.textFile("src/main/resources/input_05.txt")

  sealed trait PolarUnitCouple

  case class PolarPowerCouple() extends PolarUnitCouple

  case class NoPolarCouple() extends PolarUnitCouple

  object PolarUnitCouple {
    def apply(c1:Char,c2:Char): PolarUnitCouple = {
      if((c1.isUpper &&  c2.isLower && c1.equals(c2.toUpper))
        ||  (c1.isLower && c2.isUpper && c1.equals(c2.toLower))){
        return new PolarPowerCouple
      }else
        return new NoPolarCouple
    }
  }


  def reactPolarity(t:List[Char],c:Char):List[Char] = {
    t.size match {
      case 0 => c :: t
      case _ => {
       PolarUnitCouple(c,t.head) match {
         case a:PolarPowerCouple =>  t.tail
         case b:NoPolarCouple =>  c :: t
       }
      }
    }

  }


  val solutionA = rawData.map(s=> s.toCharArray).flatMap(c => c)
    .aggregate(List[Char]())( reactPolarity,(a,b) => a ++ b).size

  val alphabet = sc.parallelize('a' to 'z').cache()

  val stringa = rawData.collect().head

  val solutionB = alphabet.map( l =>
 //val solutionB =
    stringa.replaceAll(l.toString(),"")
      .toCharArray.aggregate(List[Char]())( reactPolarity,(a,b) => a ++ b).size).min()




  println(solutionA)
  println(solutionB)

  sc.stop()

}
