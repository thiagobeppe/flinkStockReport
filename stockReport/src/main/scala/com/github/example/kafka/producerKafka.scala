package com.github.example.kafka

import java.io.{BufferedReader, FileReader}
import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer

object producerKafka extends App {
  val props = new Properties()
  props.put("bootstrap.servers","localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks", "1")

  val topic = "STOCK_REPORT_TOPIC"
  val producer = new KafkaProducer[String, String](props)

  val file = new BufferedReader(new FileReader("../datasets/FUTURES_TRADES.txt"))
  val lines = Stream.continually(file.readLine()).takeWhile(_ != null)
  try {
    lines.foreach( line => println(line))
  }
  catch {
    case exception: Exception => println(s"Message: ${exception.getMessage}, Cause: ${exception.getCause}")
  }
}
