import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.io.Source
import scala.collection.mutable
import java.io.File
import com.example.utils.TestUtils._
import com.example.utils.Constants.EVENTS
import org.apache.spark.SparkContext

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZonedDateTime}
import java.util.Locale
import java.util.regex.Pattern
import scala.collection.mutable.ArrayBuffer

object EventCount {
  System.setProperty("hadoop.home.dir", "C:/hadoop")

  private val inputFormatter = DateTimeFormatter.ofPattern("EEE,_dd_MMM_yyyy_HH:mm:ssZ", Locale.ENGLISH)
  private val inputFormatterD = DateTimeFormatter.ofPattern("EEE,_d_MMM_yyyy_HH:mm:ssZ", Locale.ENGLISH)
  private val dmyFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy'T'HH:mm:ss")
  private val dmyFormatterZ = DateTimeFormatter.ofPattern("dd-MM-yyyy'T'HH:mm:ssZ")
  private var dateTimeZ: ZonedDateTime = null
  private var dateTime: LocalDateTime = null
  private val pattern = Pattern.compile("""\{[^}]*}|\[[^]]*]|<[^>]*>|"[^"]*"|'[^']*'|\([^)]\)*|\S+""")


  //  Функция разделения строки на элементы массива Array. Включает разделение на отдельные элементы по пробелам, элементы заключенные в различные скобки и кавычки
  private def splitLineViaBrackets(input: String): Array[String] = {
    val matcher = pattern.matcher(input)
    val result = ArrayBuffer[String]()
    while (matcher.find()) {
      result += matcher.group().trim
    }
    result.toArray
  }

  //  Функция заполнения объектов Map и ArrayBuffer подходящими элементами: Map(date, qsId) & ArrayBuffer[docIds]
  private def collectMapArr(blockMap: mutable.Map[String,String], requestArray: ArrayBuffer[String],
                            fragments: Array[String], target: Any): Unit = {
    //  Фильтрация элементов, которые не соответствуют формату <База>_<НомерДокумента>
    val filteredFragments = fragments.filter { elem =>
      if (!elem.matches("^\\w+\\d+_+\\d+$"))
        true
      else false
    }
    filteredFragments.foreach { d =>
      d match {
        //  Случай, если даты записаны в формате: dd.MM.yyyy_HH:mm:ss[Z] или yyyy.MM.dd_HH:mm:ss[Z]
        case d if "(\\d{2})[/.-](\\d{2})[/.-](\\d{4})[_T]".r.findFirstIn(d).isDefined || "(\\d{4})[/.-](\\d{2})[/.-](\\d{2})[_T]".r.findFirstIn(d).isDefined =>
          var date: String = null
          if (":(\\d{2})[_+-]\\d+".r.findFirstIn(d).isDefined) {
            if ("(\\d{4})[/.-](\\d{2})[/.-](\\d{2})[_T]".r.findFirstIn(d).isDefined) {
              date = d.replaceAll("[./]", "-").replaceAll("_(\\d{2}:)", "T$1")
              dateTimeZ = ZonedDateTime.parse(date)
            }
            else {
              date = d.replaceAll("[./]", "-").replaceAll("_(\\d{2}:)", "T$1")
              dateTimeZ = ZonedDateTime.parse(date, dmyFormatterZ)
            }
            val outputFormatter = DateTimeFormatter.ofPattern("dd.MM.yyyy")
            val dateTimeOut = dateTimeZ.format(outputFormatter)
            val key = "date"
            blockMap.put(key, blockMap.getOrElse(key, dateTimeOut))
          }
          else {
            if ("(\\d{4})[/.-](\\d{2})[/.-](\\d{2})[_T]".r.findFirstIn(d).isDefined) {
              date = d.replaceAll("[./]", "-").replaceAll("_(\\d{2}:)", "T$1")
              dateTime = LocalDateTime.parse(date)
            }
            else {
              date = d.replaceAll("[./]", "-").replaceAll("_(\\d{2}:)", "T$1")
              dateTime = LocalDateTime.parse(date, dmyFormatter)
            }
            val outputFormatter = DateTimeFormatter.ofPattern("dd.MM.yyyy")
            val dateTimeOut = dateTime.format(outputFormatter)
            val key = "date"
            blockMap.put(key, blockMap.getOrElse(key, dateTimeOut))
          }
        //  Случай, если timestamp записан в формате: Sun,_dd_MMM_yyyy_HH:mm:ssZ
        case d if "[a-zA-Z]+,_\\d+_[a-zA-Z]+_\\d+_\\d+:\\d+:\\d+_.\\d+".r.findFirstIn(d).isDefined =>
          val timezone = d.replaceAll("_([+-])", "$1")
          if ("\\d+".r.findFirstIn(d).toString.length == 2)
            dateTime = LocalDateTime.parse(timezone, inputFormatter)
          else dateTime = LocalDateTime.parse(timezone, inputFormatterD)
          val outputFormatter = DateTimeFormatter.ofPattern("dd.MM.yyyy")
          val dateTimeOut = dateTime.format(outputFormatter)
          val key = "date"
          blockMap.put(key, blockMap.getOrElse(key, dateTimeOut))
        //  Проверка идентификатора События: предполагаем числовое значение длиною >= 4 символа
        case d if d.trim.length >= 4 && "^-?\\d+$".r.findFirstIn(d).isDefined =>
          val key = "eventId"
          blockMap.put(key, blockMap.getOrElse(key, d))
        case d if EVENTS.contains(d) =>
          val key = "event"
          blockMap.put(key, blockMap.getOrElse(key, d))
        case _ => 
      }
    }
    //  Элементы соответствующие формату <База>_<НомерДокумента>, кладем в массив ArrayBuffer
    requestArray ++= fragments.filter { fragment =>
      target match {
        case target: String =>
          fragment == target
        case target if target.getClass.isArray =>
          target.asInstanceOf[Array[String]].contains(fragment)
      }
    }
  }

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
