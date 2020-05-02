package com.github.example

import java.util.Properties

import com.github.example.filters.trackChange
import com.github.example.utils.timeStampExtract
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object stockReport extends App {
    val env =  StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    env.setParallelism(1)

    val data = env.readTextFile("../datasets/FUTURES_TRADES.txt")
                    .map(value => {val fiels = value.split(",");(fiels(0), fiels(1), "XYZ", fiels(2).toDouble, fiels(3).toInt)})
                    .assignTimestampsAndWatermarks(new timeStampExtract())

    val changeData = data
                        .keyBy(value => value._3)
                        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                        .process(new trackChange()).print()

    env.execute()
}
