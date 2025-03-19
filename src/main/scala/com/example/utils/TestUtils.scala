package com.example.utils

import com.example.utils.Constants.EVENTS
import java.io.File
import scala.io.Source

object TestUtils {

  def checkPath(path: String): Unit = {
    val file = new File(path)
    if (!file.isFile && !file.isDirectory){
      throw new IllegalArgumentException(s"Path $path is not a directory path ot path to file!")
    }
    else if (!file.exists()){
      throw new IllegalArgumentException(s"Path $path is not existed or written wrong!")
    }
    else if (file.isDirectory){
      if (file.listFiles().isEmpty){
        throw new IllegalArgumentException(s"Directory $path should not be empty!")
      }
      else {
        file.listFiles().foreach{ element =>
          if (!element.canRead)
            throw new IllegalArgumentException(s"Directory $path have not readable files: $element")
        }
      }
    }
    else if (file.isFile){
      if (!file.canRead)
        throw new IllegalArgumentException(s"File $path is not readable!")
    }
  }

  def checkParam(arg: String): Unit={
    if (arg == null || arg.trim.isEmpty)
      throw new IllegalArgumentException(s"Search parameters must not be empty!")
    val file = new File(arg)
    if (file.isFile) {
      if (!file.exists())
        throw new IllegalArgumentException(s"Path to file via search parameters $arg is not existing!")
      else{
        if (!file.canRead)
          throw new IllegalArgumentException(s"File $arg is not readable!")
        else {
          val params = Source.fromFile(arg).getLines().toArray
          checkParam(params)
        }
      }
    }
  }
  def checkParam(arg: Array[String]): Unit={
    if (arg.isEmpty)
      throw new IllegalArgumentException(s"Search parameters must not be empty!")
    arg.foreach(elem => checkParam(elem))
  }

  def checkEvent(arg: String): Unit={
    if (!EVENTS.contains(arg)){
      throw new IllegalArgumentException(s"This type of Event doesn't exists!")
    }
  }
}