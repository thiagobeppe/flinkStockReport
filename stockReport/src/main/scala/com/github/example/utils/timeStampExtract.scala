package com.github.example.utils

import java.text.SimpleDateFormat
import java.sql.Timestamp

import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor

class timeStampExtract extends AscendingTimestampExtractor[(String,String,String,Double,Int)]{
  private val dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
  override def extractAscendingTimestamp(element: (String, String, String, Double, Int)): Long = {
    try {
      val ts = new Timestamp(dateFormat.parse(element._1 + " " + element._2).getTime)
      ts.getTime
    }
    catch {
      case e: Exception => throw new java.lang.RuntimeException("Parsing Error")
    }

  }
}
