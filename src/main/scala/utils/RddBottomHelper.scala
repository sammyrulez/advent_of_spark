package utils

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * This is a utility class by Eyal Edelman I used to read the "tail" records of an RDD
  * http://www.swi.com/spark-rdd-getting-bottom-records/
  */

object RddBottomHelper {

  case class PartitionInfo(id: Int, startRecord: Long, lastRecord: Long  )
  case class RddInfo(size: Long, partitionsInto: List[PartitionInfo])

  /* geting the size as Long not as Int froum size...*/
  def getIteratorSize[T](iterator: Iterator[T]): Long = {
    iterator.map(x => 1L).sum
  }

  def getRddInfo[T](rdd: RDD[T]): RddInfo = {
    rdd.mapPartitionsWithIndex( (id, itr) => Seq( ( id, getIteratorSize(itr) ) ).toIterator )
      .sortByKey().collect()
      .foldLeft(RddInfo(0L, List.empty))({
        case ( info, (id, size)) =>
          RddInfo(info.size + size, PartitionInfo(id, info.size, info.size + size) :: info.partitionsInto )
      })
  }

  def bottom[T: ClassTag](rdd: RDD[T], numRecords: Int): Array[T] = {

    if (numRecords < 1) return Array[T]()

    rdd.cache()
    val rddInfo = getRddInfo(rdd)

    val endRecord = rddInfo.size - 1
    val startRecord = endRecord - numRecords + 1
    val mapInfo = rddInfo.partitionsInto.filter(x=> x.lastRecord > startRecord).map(x => (x.id, x)).toMap

    rdd.mapPartitionsWithIndex( (id, itr) =>
      if (mapInfo.contains(id)) {
        val dropRecords = startRecord - mapInfo.get(id).get.startRecord
        itr.drop(dropRecords.toInt)
      }
      else Iterator.empty
    ).collect()
  }
}
