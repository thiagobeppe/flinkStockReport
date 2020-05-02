package com.github.example

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object stockReport extends App {
    val env =  StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    env.execute()
}
