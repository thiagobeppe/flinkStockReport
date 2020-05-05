package com.github.example.filters

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class trackChange extends ProcessWindowFunction[(String,String,String,Double,Int), String, String, TimeWindow] {
  @transient
  private var prevWindowMaxTrade: ValueState[Double] = _
  @transient
  private var prevWindowsMaxVol:  ValueState[Int] = _


  override def open(parameters: Configuration): Unit = {
    prevWindowMaxTrade = getRuntimeContext.getState(new ValueStateDescriptor[Double]("prev_max_trade", classOf[Double], 0.0))
    prevWindowsMaxVol = getRuntimeContext.getState(new ValueStateDescriptor[Int]("prev_max_vol", classOf[Int], 0))
  }

  override def process(key: String,
                       context: Context,
                       elements: Iterable[(String, String, String, Double, Int)],
                       out: Collector[String]): Unit = {

    var windowStart = ""
    var windowEnd = ""
    var windowMaxTrade = 0.0
    var windowMinTrade = 0.0
    var windowMaxVol = 0
    var windowMinVol = 0
    elements.foreach(
      element => {
        if (windowStart.isEmpty) {
          windowStart = element._1 + ":" + element._2
          windowMinTrade = element._4
          windowMinVol = element._5
        }
        if (element._4 > windowMaxTrade) {
          windowMaxTrade = element._4
        }
        if (element._5 > windowMaxVol) {
          windowMaxVol = element._5
        }
        if (element._4 < windowMinTrade) {
          windowMinTrade = element._4
        }
        if (element._5 < windowMinVol) {
          windowMinVol = element._5
        }
        windowEnd = element._1 + ":" + element._2
      })
    var maxTradeChange = 0.0
    var maxVolChange = 0.0

    if(prevWindowMaxTrade.value() != 0){
      maxTradeChange = (windowMaxTrade - prevWindowMaxTrade.value())/ prevWindowMaxTrade.value() * 100
    }
    if(prevWindowsMaxVol.value() != 0){
      maxVolChange = (maxVolChange - prevWindowsMaxVol.value())/ prevWindowsMaxVol.value() * 100
    }



    out.collect(s"$windowStart - $windowEnd , $windowMaxTrade, $windowMinTrade, %.2f , $windowMaxVol, $windowMinVol , %.2f".format(maxTradeChange, maxVolChange))
    prevWindowsMaxVol.update(windowMaxVol)
    prevWindowMaxTrade.update(windowMaxTrade)
  }}