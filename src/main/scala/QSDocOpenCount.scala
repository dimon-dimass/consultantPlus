import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import com.example.utils.TestUtils._
import com.example.utils.Constants.EVENTS
import java.time.format._
import java.time._
import java.util.Locale
import java.util.regex.Pattern
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.{FileSystem, Path}

object QSDocOpenCount {
  System.setProperty("hadoop.home.dir", "C:/hadoop")
//  Объекты-паттерны для дат и строковых преобразований
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
  private def collectMapArr(blockMap: mutable.Map[String,String],
                            requestArray: ArrayBuffer[String], fragments: Array[String]): Unit = {
//  Фильтрация элементов, которые не соответствуют формату <База>_<НомерДокумента>
    val filteredFragments = fragments.filter{ elem =>
      if (!elem.matches("^\\w+\\d+_+\\d+$"))
        true
      else false
    }
    filteredFragments.foreach{ d =>
      d match {
//  Случай, если даты записаны в формате: dd.MM.yyyy_HH:mm:ss[Z] или yyyy.MM.dd_HH:mm:ss[Z]
        case d if "(\\d{2})[/.-](\\d{2})[/.-](\\d{4})[_T]".r.findFirstIn(d).isDefined || "(\\d{4})[/.-](\\d{2})[/.-](\\d{2})[_T]".r.findFirstIn(d).isDefined =>
          var date: String = null
          if (":(\\d{2})[_+-]\\d+".r.findFirstIn(d).isDefined) {
            if ("(\\d{4})[/.-](\\d{2})[/.-](\\d{2})[_T]".r.findFirstIn(d).isDefined) {
              date = d.replaceAll("[./]", "-").replaceAll("_(\\d{2}:)","T$1")
              dateTimeZ = ZonedDateTime.parse(date)
            }
            else {
              date = d.replaceAll("[./]", "-").replaceAll("_(\\d{2}:)","T$1")
              dateTimeZ = ZonedDateTime.parse(date, dmyFormatterZ)
            }
            val outputFormatter = DateTimeFormatter.ofPattern("dd.MM.yyyy")
            val dateTimeOut = dateTimeZ.format(outputFormatter)
            val key = "date"
            blockMap.put(key, blockMap.getOrElse(key,dateTimeOut))
          }
          else{
            if ("(\\d{4})[/.-](\\d{2})[/.-](\\d{2})[_T]".r.findFirstIn(d).isDefined) {
              date = d.replaceAll("[./]", "-").replaceAll("_(\\d{2}:)","T$1")
              dateTime = LocalDateTime.parse(date)
            }
            else {
              date = d.replaceAll("[./]", "-").replaceAll("_(\\d{2}:)","T$1")
              dateTime = LocalDateTime.parse(date, dmyFormatter)
            }
            val outputFormatter = DateTimeFormatter.ofPattern("dd.MM.yyyy")
            val dateTimeOut = dateTime.format(outputFormatter)
            val key = "date"
            blockMap.put(key, blockMap.getOrElse(key,dateTimeOut))
          }
//  Случай, если timestamp записан в формате: Sun,_dd_MMM_yyyy_HH:mm:ssZ
        case d if "[a-zA-Z]+,_\\d+_[a-zA-Z]+_\\d+_\\d+:\\d+:\\d+_.\\d+".r.findFirstIn(d).isDefined =>
          val timezone = d.replaceAll("_([+-])", "$1")
          if ("\\d+".r.findFirstIn(d).toString.length == 2)
            dateTime = LocalDateTime.parse(timezone, inputFormatter)
          else dateTime = LocalDateTime.parse(timezone,inputFormatterD)
          val outputFormatter = DateTimeFormatter.ofPattern("dd.MM.yyyy")
          val dateTimeOut = dateTime.format(outputFormatter)
          val key = "date"
          blockMap.put(key, blockMap.getOrElse(key,dateTimeOut))
//  Проверка идентификатора События: предполагаем числовое значение длиною >= 4 символа
        case d if d.trim.length >= 4 && "^-?\\d+$".r.findFirstIn(d).isDefined =>
          val key = "eventId"
          blockMap.put(key, blockMap.getOrElse(key,d))
        case _ =>
      }
    }
//  Элементы соответствующие формату <База>_<НомерДокумента>, кладем в массив ArrayBuffer
    requestArray ++= fragments.filter{ elem =>
      if (elem.matches("^\\w+\\d+_+\\d+$"))
        true
      else false
    }
  }
//  inB: Boolean, inOB: Boolean,
//  blockMap: mutable.Map[String,String], requestArray: ArrayBuffer[String],
//
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
  private def qsDocOpenCount(sc: SparkContext, logsPath: String): Unit = {
    val logsData = sc.textFile(logsPath, minPartitions = 10) // формируем RDD

//  Формируем RDD  для QuickSearch (QS) блоков в виде: (date, qsId, docId, 0)
    val qsRDD = logsData.mapPartitions { line =>
      formEventRDD(line, "QS", 0)
    }.collect{case (date, qsId, docId, count) => ((date, qsId, docId), count)}.cache()

    if (qsRDD.isEmpty()){
      throw new Exception(s"Error to compute RDD of QuickSearch (QS) Events either in input file/s or in computation")
    }

//  Принцип идентичен qsRDD, но теперь для DOC_OPEN блоков RDD принимает вид: (date, qsId, docId, 1)
    val doRDD = logsData.mapPartitions { line =>
      formEventRDD(line,"DOC_OPEN", 1)
    }.collect{case (date, qsId, docId, count) => ((date, qsId, docId), count)}.cache()

    if (doRDD.collect().isEmpty){
      throw new Exception(s"Error to compute RDD of DocumentOpen (DO) Events either in input file/s or in computation")
    }

    // Соединение двух RDD по ключу (date, qsId, docId)
    val mergedRDD = qsRDD.join(doRDD).map{case (key, (_,count)) => (key, count)}
    val result = mergedRDD.reduceByKey(_ + _).sortByKey().map { case ((date, qsId, docId), count) => s"${date.get}, ${qsId.get}, $docId, $count" }

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val outputPath = new Path("output/task2")
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true)
    }
    result.coalesce(1).saveAsTextFile("output/task2")
  }

  def main(args: Array[String]): Unit= {

    checkPath(args(0))

    val logsPath = args(0)
    val spark = SparkSession.builder()
      .appName("CardSearch Application")
      .master("local[4]")
      .config("spark.sql.warehouse.dir", "file:///C:/tmp/hive")
      .getOrCreate()
    val sc = spark.sparkContext

    qsDocOpenCount(sc, logsPath)

    spark.stop
  }

}
