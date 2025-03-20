package com.example.utils

import com.example.utils.Constants.EVENTS

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZonedDateTime}
import java.util.Locale
import java.util.regex.Pattern
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Utils {
  private val inputFormatter = DateTimeFormatter.ofPattern("EEE,_dd_MMM_yyyy_HH:mm:ssZ", Locale.ENGLISH)
  private val inputFormatterD = DateTimeFormatter.ofPattern("EEE,_d_MMM_yyyy_HH:mm:ssZ", Locale.ENGLISH)
  private val dmyFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy'T'HH:mm:ss")
  private val dmyFormatterZ = DateTimeFormatter.ofPattern("dd-MM-yyyy'T'HH:mm:ssZ")
  private var dateTimeZ: ZonedDateTime = null
  private var dateTime: LocalDateTime = null
  private val pattern = Pattern.compile("""\{[^}]*}|\[[^]]*]|<[^>]*>|"[^"]*"|'[^']*'|\([^)]\)*|\S+""")


  //  Функция разделения строки на элементы массива Array. Включает разделение на отдельные элементы по пробелам, элементы заключенные в различные скобки и кавычки
  def splitLineViaBrackets(input: String): Array[String] = {
    val matcher = pattern.matcher(input)
    val result = ArrayBuffer[String]()
    while (matcher.find()) {
      result += matcher.group().trim
    }
    result.toArray
  }

  //  Функция заполнения объектов Map и ArrayBuffer подходящими элементами: Map(date, qsId) & ArrayBuffer[docIds]
  def collectMapArr(blockMap: mutable.Map[String,String], requestArray: ArrayBuffer[String],
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

  def collectMapArr(blockMap: mutable.Map[String,String],
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

}
