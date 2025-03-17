import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.PrintWriter

object Main {
  System.setProperty("hadoop.home.dir", "C:/hadoop")

  def main(args: Array[String]): Unit= {
    val logsPath = "E:/ConsultantPlus/sessions/*"
    val spark = SparkSession.builder()
      .appName("Main Application")
      .master("local[4]")
      .config("spark.sql.warehouse.dir", "file:///C:/tmp/hive")
      .getOrCreate()
    val sc = spark.sparkContext
    val logsData = sc.wholeTextFiles(logsPath) // формируем датасеты (logPath, logContent)

    // функция решения задачи №2
    def qsDocOpenCount(): Unit = {
      // преобразуем датасет (logPath, logContent) в датасет ((logDate, docId), count)
      val dateData = logsData.flatMap {
        case (_, content) =>
          val lines = content.split("\n")
          val logDate = lines(0).split(" ")(1).split("_")(0)
          // помещаем в переменную id всех qs
          val qsIds = (for (i <- 0 until lines.length - 1 if lines(i).startsWith("QS"))
            yield lines(i + 1).split(" ")(0)
            ).toSet
          // создаем словарь, в котором будем подсчитывать для сессии количество открытий докуметов
          var countsMap = Map[(String, String), Int]()
          lines.foreach { line =>
            if (line.startsWith("DOC_OPEN")) {
              val elems = line.split(" ")
              if (qsIds.contains(elems(2))) {
                val key = (logDate, elems(3)) // elems(3) - является идентификатором документа
                countsMap = countsMap + (key -> (countsMap.getOrElse(key, 0) + 1))
              }
            }
          }
          countsMap
      }
      val result = dateData.reduceByKey(_ + _).sortByKey().map { case ((logDate, docId), count) => s"$logDate, $docId, $count" }

      val fs = FileSystem.get(sc.hadoopConfiguration)
      val outputPath = new Path("output/task2")
      if (fs.exists(outputPath)) {
        fs.delete(outputPath, true)
      }
      result.saveAsTextFile("output/task2")
    }


    //проверка существуют ли сессии с различными днями старта и окончания (нет)
//    val apartSess = logsData.filter{
//      case (_, content) =>
//        val cntsplt = content.split("\n")
//        val start = cntsplt(0).split(" ")(1).split(".").slice(0,3)
//        val end = cntsplt(cntsplt.length-1).split(" ")(1).split(".").slice(0,3)
//        start.sameElements(end)
//    }


    // функция решения задачи №1
    def cardSearchCount(targetId: String): Long = {

      // в следующей переменной формируем датасеты содержащие только блоки CARD_SEARCH в аналогичном формате
      val cardSearchData = logsData.flatMap { case (filePath, content) =>

        val lines = content.split("\n")
        var cardSearch = scala.collection.mutable.ArrayBuffer[String]()
        var inCSB = false
        var current = ""
        for (line <- lines) {
          if (line.startsWith("CARD_SEARCH_START")) {
            inCSB = true
            current = line
          } else if (line.startsWith("CARD_SEARCH_END")) {
            inCSB = false
            current += "\n" + line
            cardSearch += current
            current = ""
          } else if (inCSB) {
            current += "\n" + line
          }
        }
        // в случае, если в логе не было соответствующего блока, не записываем (None) в новый датасет
        if (cardSearch.isEmpty)
          None
        else {
          cardSearch.map( block => (filePath, block) )
        }
      }
      // производим фильтрацию по необходимому идетификатору
      val targetData = cardSearchData.filter { case (_, block) =>
        block.contains(targetId)
      }
      val tDataCount = targetData.count()
      val writer = new PrintWriter(s"output/task1/$targetId.txt")
      writer.println(s"Количество раз, когда в карточке производили поиск документа с идентификатором $targetId: $tDataCount")
      writer.close()
      println(s"COUNT: $tDataCount")
      tDataCount
    }
    qsDocOpenCount()
    cardSearchCount("ACC_45616")
    println(s"ЗАДАЧА №1 & №2 \nДля просмотра результатов перейдите в /output и просмотрите результаты в TXT и CSV формате, соответственно")
    spark.stop
  }

}

