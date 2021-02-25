package com.flink.test

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object TestStreamJob {
   case class SensorReading(id:String, timeStamp: Long, temperature: Double)

  class TimestampExtractor extends AssignerWithPeriodicWatermarks[SensorReading] with Serializable {
    private var currentMaxTimestamp = 0L
    private val maxOutOfOrderness = 5000l
    override def extractTimestamp(e: SensorReading, prevElementTimestamp: Long) : Long = {
      val timestamp=e.timeStamp*1000
      currentMaxTimestamp = Math.max(prevElementTimestamp,timestamp)
      timestamp
    }
    override def getCurrentWatermark(): Watermark = {
      println(currentMaxTimestamp-maxOutOfOrderness)
      new Watermark(currentMaxTimestamp-maxOutOfOrderness)
    }
  }

  def main(args: Array[String]): Unit = {
    // 获取执行环境

//    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration())
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(1)
    // 设置全局的并行度为1，方便测试
    environment.setParallelism(1)
    // 设置时间语义为事件时间
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 从socket中获取数据
    val inputStream2 = environment.socketTextStream("k8s-211", 4444)

    // 将源数据转换成样例类类型，设置waterMark延迟时间和事件时间字段
    val dataStream: DataStream[SensorReading] = inputStream2.map(data => {
      val arr = data.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    }).assignTimestampsAndWatermarks(new TimestampExtractor)


    // 创建侧输出流
    val latetag = new OutputTag[(String, Double, Long)]("late")

    // 每15秒统计一次，窗口内各传感器所有温度的最小值，以及最新的时间戳
    val resultStream: DataStream[(String, Double, Long)] = dataStream
      .map(data => (data.id, data.temperature, data.timeStamp))
      .keyBy(_._1) // 根据id分组
      .timeWindow(Time.seconds(10)) // 时间滚动窗口，窗口大小15秒
      .allowedLateness(Time.minutes(1)) // 允许处理迟到的数据
      .sideOutputLateData(latetag) // 将迟到的数据放入侧输出流
      .reduce(
      // （String, Double, Long） id,最小温度，最新时间戳
      (currRes, newDate) => (currRes._1, currRes._2.min(newDate._2), newDate._3)
    )

    resultStream.getSideOutput(latetag).print("late")
    resultStream.print("window result")

    // 执行
    environment.execute("window test")
  }

/**
  * test data
sensor_1,1547718199,35.8
sensor_6,1547718201,15.4
sensor_7,1547718202,6.7
sensor_10,1547718205,38.1
sensor_1,1547718206,29.3
sensor_1,1547718209,38.3
sensor_1,1547718210,33.6
sensor_1,1547718213,36.6
  */
}
