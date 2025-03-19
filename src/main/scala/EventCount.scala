import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.io.Source
import java.io.File
import com.example.utils.TestUtils._
import com.example.utils.Constants.EVENTS
import org.apache.spark.SparkContext
import java.util.regex.Pattern
import scala.collection.mutable.ArrayBuffer

object EventCount {
  System.setProperty("hadoop.home.dir", "C:/hadoop")
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

  private def formEventRDD(iterator: Iterator[String], event: String, target: Any): Iterator[((String, String), Int)] = {
    var blockArray = ArrayBuffer[String]()
  // Переменная-флаг, говорит о том, что нового события еще не наступило и можно продолжать обрабатывать строки
    var inEB = false

    iterator.flatMap { line =>
      val blockArray = ArrayBuffer[String]()
      // Разделение строки на элементы
      val fragments = splitLineViaBrackets(line)

      // Проверяем является ли строка началом искомого События
      if (fragments.contains(EVENTS(event))){
        inEB = true
        //        println("ВОШЛИ В БЛОК")
        // В переменную blockArray записываются только те элементы, которые:
        // Не являются: датой, id операции (предполагаются числовым значением длиной более 7 символов) и событием
        // Являются записью вида: <БАЗА>_<НОМЕР ДОКУМЕНТА>
        blockArray ++= fragments.filter{ elem =>
          if(!("(\\d{2})[/.-](\\d{2})[/.-](\\d{4})[_T]".r.findFirstIn(elem).isDefined
            || "(\\d{4})[/.-](\\d{2})[/.-](\\d{2})[_T]".r.findFirstIn(elem).isDefined
            || "[a-zA-Z]+,_\\d+_[a-zA-Z]+_\\d+_\\d+:\\d+:\\d+_.\\d+".r.findFirstIn(elem).isDefined)
            && !(elem.trim.length >= 4 && "^-?\\d+$".r.findFirstIn(elem).isDefined)
            && !(elem == event)
            && elem.matches("^\\w+\\d+_+\\d+$"))
            true
          else false
        }

        // Если blockArray не пуст, то проверяем содержится ли в нем искомое/ые Событие/ия
        // и передаем на выход flatMap'а в формате ((Событие, Идентификатор документа), 1)
        if (blockArray.nonEmpty) {
          target match {
            case target: String =>
              if (blockArray.contains(target)) {
                inEB = false
                blockArray.filter(_==target).map{target => ((event, target),1)}
              }
              else Seq.empty
            case target: Array[String] =>
              if (blockArray.exists(target.contains(_))) {
                inEB = false
                blockArray.filter(target.contains).map{target => ((event, target), 1)}
              }
              else Seq.empty
          }
        }
        else {
          Seq.empty
        }
      }
      // Проверка для событий с парными значениями (алгоритм вывода тот же)
      else if (inEB && (fragments.forall(!EVENTS.contains(_)) || fragments.contains(EVENTS("CARD_SEARCH_END")))){
        blockArray ++= fragments.filter{ elem =>
          if (!elem.matches("(\\d{2})[/.-](\\d{2})[/.-](\\d{4})") &&
            !(elem.trim.length >= 8 && elem.matches("^\\d+$")) &&
            !(elem == event) &&
            elem.matches("\\w+_+\\d+"))
            true
          else false
        }
        if (blockArray.nonEmpty) {
          target match {
            case target: String =>
              if (blockArray.contains(target)) {
                inEB = false
                blockArray.filter(_==target).map{target => ((event, target),1)}
              }
              else None
            case target: Array[String] =>
              if (blockArray.exists(target.contains(_))) {
                inEB = false
                blockArray.filter(target.contains).map{target => ((event, target),1)}
              }
              else Seq.empty
          }
        }
        else {
          Seq.empty
        }
      }
      // В случае выхода из блока события или не нахождения требуемого очищаем все переменные и ничего не выводим
      else{
        inEB = false
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
      val targetData = eventData.reduceByKey(_ + _).map { case ((event, target), count) => (event, target, count) }.cache()

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
