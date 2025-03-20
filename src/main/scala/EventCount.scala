import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.io.Source
import scala.collection.mutable
import java.io.File
import com.example.utils.TestUtils._
import com.example.utils.Constants.EVENTS
import com.example.utils.Utils._
import org.apache.spark.SparkContext

import scala.collection.mutable.ArrayBuffer

object EventCount {
  System.setProperty("hadoop.home.dir", "C:/hadoop")

  private def formEventRDD(iterator: Iterator[String], event: String, target: Any): Iterator[(Option[String], Option[String], Option[String], String, Int)] = {
    var requestArray = ArrayBuffer[String]()
    val blockMap = mutable.Map[String, String]()

    // Переменная-флаг, говорит о том, что нового события еще не наступило и можно продолжать обрабатывать строки
    var inB = false
    var inOB = false

    iterator.flatMap { line =>
      val fragments = splitLineViaBrackets(line)
      if (fragments.contains(EVENTS(event))) {
        inB = true
        inOB = false
        collectMapArr(blockMap, requestArray, fragments, target)
        //  В случае, если в данной строке были заполнены объект Map(date, qsId) и ArrayBuffer[docIds], выводим каждое сочетание, иначе ничего
        if (blockMap.size > 1 && requestArray.nonEmpty) {
          val flatArray = requestArray.map { docId => (blockMap.get("event"), blockMap.get("date"), blockMap.get("eventId"), docId, 1) }
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
        collectMapArr(blockMap, requestArray, fragments, target)
        //  В случае, если в данной строке были заполнены объект Map(date, qsId) и ArrayBuffer[docIds], выводим каждое сочетание, иначе ничего
        if (blockMap.size > 1 && requestArray.nonEmpty) {
          val flatArray = requestArray.map { docId => (blockMap.get("event"), blockMap.get("date"), blockMap.get("eventId"), docId, 1) }
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

// Функция решения задачи №1
  private def eventCount(sc: SparkContext, logsPath: String, event: String, target: Any): Unit = {
//    (String, String, Long)
    val logsData = sc.textFile(logsPath, minPartitions = 10) // формируем RDD


    // В следующей переменной формируем RDD содержащие только блоки CARD_SEARCH в аналогичном формате
    val eventData = logsData.mapPartitions { line =>
        formEventRDD(line, event, target)
    }

// проверяем на пустоту RDD и в случае пустого RDD заполняем нулями искомые идентификаторы и записываем в файл
    if (eventData.isEmpty) {
      println("Пустая коллекция")
      target match{
        case target: String =>
          val outputData = eventData.map{ _ => (event,target,0)}
          outputData.coalesce(1).saveAsTextFile("output/task1")
        case target if target.getClass.isArray =>
          val outputData = sc.parallelize(target.asInstanceOf[Array[String]])
          outputData.map{ t => (event,t,0)}
          outputData.coalesce(1).saveAsTextFile("output/task1")
      }
    }
    else {
      val clearedData = eventData.mapPartitions{ partition =>
        partition.toSet.iterator
      }.map{ case (event, _, _, docId, count) => ((event, docId), count)}
      val targetData = clearedData.reduceByKey(_ + _).map { case ((event, target), count) => (event, target, count) }.cache()

      val fs = FileSystem.get(sc.hadoopConfiguration)
      val outputPath = new Path("output/task1")
      if (fs.exists(outputPath)) {
        println(s"Для записи файла директория ${outputPath.toString} будет очищена")
        fs.delete(outputPath, true)
      }
      targetData.map { case (event, target, count) => s"$event, $target, $count" }.coalesce(1).saveAsTextFile("output/task1")
    }
  }

  def main(args: Array[String]): Unit= {
    if (args.length < 2)
      throw new IllegalArgumentException(s"Provide some arguments!")
// проверка корректности пути и читабельности файлов
    checkPath(args(0))
// проверка корректности имени События
    checkEvent(args(1))
// проверка корректности Параметров запроса
    checkParam(args(2))


    val logsPath = args(0)
    val event = args(1)
// переменная t может быть определена либо типом String, либо структурой Array[String]
    var t: Option[Any] = None
    println(s"args(2): ${args(2)}")
    if (new File(args(2)).isFile) {
      println("target is file")
      t = Some(Source.fromFile(args(2)).getLines().toArray)
    }
    else if (args(2).split(",").length > 1) {
      println("target is array")
      t = Some(args(2).split(","))
    }
    else {
      println("target is string")
      t = Some(args(2))
    }
    val target = t.get
    println(s"TARGET = $target")

    val spark = SparkSession.builder()
      .appName("CardSearch Application")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/tmp/hive")
      .getOrCreate()
    val sc = spark.sparkContext

    eventCount(sc, logsPath, event, target)

//    println(s"ЗАДАЧА №1 & №2 \nДля просмотра результатов перейдите в /output и просмотрите результаты в TXT и CSV формате, соответственно")
    spark.stop
  }

}
