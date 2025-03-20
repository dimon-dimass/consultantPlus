import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import com.example.utils.TestUtils._
import com.example.utils.Constants.EVENTS
import com.example.utils.Utils._

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.{FileSystem, Path}

object QSDocOpenCount {
  System.setProperty("hadoop.home.dir", "C:/hadoop")

  private def formEventRDD(iterator: Iterator[String], event: String, count: Int): Iterator[(Option[String],Option[String],String, Int)] = {
  //  Разделение строки на элементы массива
  //  inbB - флаг нахождения обработчика в Событии
    var inB = false
  //  inOB - флаг нахождения обработчика в Событии, отличном от требуемого
    var inOB = false
    val blockMap = mutable.Map[String, String]()
    val requestArray = ArrayBuffer[String]()

    iterator.flatMap { line =>
      val fragments = splitLineViaBrackets(line)
      if (fragments.contains(EVENTS(event))) {
        inB = true
        inOB = false
        collectMapArr(blockMap, requestArray, fragments)
        //  В случае, если в данной строке были заполнены объект Map(date, qsId) и ArrayBuffer[docIds], выводим каждое сочетание, иначе ничего
        if (blockMap.size > 1 && requestArray.nonEmpty) {
          val flatArray = requestArray.map { docId => (blockMap.get("date"), blockMap.get("eventId"), docId, count) }
          requestArray.clear()
          flatArray
        }
        else Seq.empty
      }
      else if (fragments.forall(!EVENTS.contains(_))) {
        inOB = true
        Seq.empty
      }
      else if (inB) {
        collectMapArr(blockMap, requestArray, fragments)
        //  В случае, если в данной строке были заполнены объект Map(date, qsId) и ArrayBuffer[docIds], выводим каждое сочетание, иначе ничего
        if (blockMap.size > 1 && requestArray.nonEmpty) {
          val flatArray = requestArray.map { docId => (blockMap.get("date"), blockMap.get("eventId"), docId, count) }
          requestArray.clear()
          flatArray
        }
        else Seq.empty
      }
      else {
        //  Если в блоке События не было даты или qsId, то либо проблема данных, либо ошибка обработчика (прекращение работы программы, либо пропуск строки)
        if (blockMap.size < 2 && blockMap.nonEmpty) {
          throw new Exception(s"Что-то не так, проблема с данными $blockMap, $requestArray")
          //          None
        }
        blockMap.clear()
        requestArray.clear()
        inB = false
        Seq.empty
      }
    }
  }

//  Функция решения задачи №2
  private def qsDocOpenCount(sc: SparkContext, logsPath: String, searchEvent: String, docOpen: String): Unit = {
    val logsData = sc.textFile(logsPath, minPartitions = 10) .cache()// формируем RDD

//  Формируем RDD  для QuickSearch (QS) блоков в виде: (date, qsId, docId, 0)
    val seRDD = logsData.mapPartitions { line =>
      formEventRDD(line, searchEvent, 0)
    }.collect{case (date, qsId, docId, count) => ((date, qsId, docId), count)}.cache()

    if (seRDD.isEmpty()){
      throw new Exception(s"Error to compute RDD of QuickSearch (QS) Events either in input file/s or in computation")
    }

//  Принцип идентичен qsRDD, но теперь для DOC_OPEN блоков RDD принимает вид: (date, qsId, docId, 1)
    val doRDD = logsData.mapPartitions { line =>
      formEventRDD(line,docOpen, 1)
    }.collect{case (date, qsId, docId, count) => ((date, qsId, docId), count)}.cache()

    if (doRDD.collect().isEmpty){
      throw new Exception(s"Error to compute RDD of DocumentOpen (DO) Events either in input file/s or in computation")
    }

    val clearedDataSE = seRDD.mapPartitions{ partition =>
      partition.toSet.iterator
    }
    val clearedDataDO = doRDD.mapPartitions{ partition =>
      partition.toSet.iterator
    }

    // Соединение двух RDD по ключу (date, qsId, docId)
    val mergedRDD = clearedDataSE.join(clearedDataDO).map{case (key, (_,count)) => (key, count)}
    val result = mergedRDD.reduceByKey(_ + _).sortByKey().map { case ((date, qsId, docId), count) => s"${date.get}, ${qsId.get}, $docId, $count" }

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val outputPath = new Path("output/task2")
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true)
    }
    result.coalesce(1).saveAsTextFile("output/task2")
  }

  def main(args: Array[String]): Unit= {

    checkPath(args(0).trim)

    checkEvent(args(1).trim)

    if (!args(2).trim.matches(EVENTS("DOC_OPEN"))){
      throw new IllegalArgumentException(s"""Third argument must be Event "DOC_OPEN" """)
    }

    val logsPath = args(0)
    val search = args(1)
    val docOpen = args(2)

    val spark = SparkSession.builder()
      .appName("CardSearch Application")
      .master("local[4]")
      .config("spark.sql.warehouse.dir", "file:///C:/tmp/hive")
      .getOrCreate()
    val sc = spark.sparkContext

    qsDocOpenCount(sc, logsPath, search, docOpen)

    spark.stop
  }

}
