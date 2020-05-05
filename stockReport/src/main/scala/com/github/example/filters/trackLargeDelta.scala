package com.github.example.filters

import org.apache.flink.api.common.state.ValueState
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class trackLargeDelta(thresholdValue: Double) extends ProcessWindowFunction[(String,String,String,Double,Int), String, String, TimeWindow]{
  val threshold = this.thresholdValue
  @transient
  private var prevWindowMaxTrade: ValueState[Double] = _

  override def process(key: String,
                       context: Context,
                       elements: Iterable[(String, String, String, Double, Int)],
                       out: Collector[String]): Unit = {
    var prevMax = prevWindowMaxTrade.value()
    var curMax = 0.0
    var currMAaxTimeStamp = ""
    elements.foreach( element => {
      if(element._4 > curMax){
        curMax = element._4
        currMAaxTimeStamp = s"${element._1} : ${element._2}"
      }
    })
    var maxPriceChange = ((curMax - prevMax)/prevMax) *100
    if (prevMax != 0 && Math.abs(maxPriceChange) > threshold){
      out.collect(s"Large Change detected of %.2f % ($prevMax - $curMax) at $currMAaxTimeStamp".format(maxPriceChange))
    }
    prevWindowMaxTrade.update(curMax)
  }
}
