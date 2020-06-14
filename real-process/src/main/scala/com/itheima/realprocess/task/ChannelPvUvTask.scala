package com.itheima.realprocess.task

import com.itheima.realprocess.bean.ClickLogWide
import com.itheima.realprocess.util.HBaseUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

case class ChannelPvUv(
                      channelId:String,
                      yearMonthDayHour:String,
                      pv:Long,
                      uv:Long
                      )

object ChannelPvUvTask {


  def process(clickLogWideDataStream: DataStream[ClickLogWide]) = {
    // 1.转换 map是一种类型转换为另一种类型 flatmap 是一种转多种
    val channelPvUvDS: DataStream[ChannelPvUv] = clickLogWideDataStream.flatMap {
      clickLogWide => {
        List(
          ChannelPvUv(clickLogWide.channelID, clickLogWide.yearMonthDayHour, clickLogWide.count, clickLogWide.isHourNew),
          ChannelPvUv(clickLogWide.channelID, clickLogWide.yearMonthDay, clickLogWide.count, clickLogWide.isDayNew),
          ChannelPvUv(clickLogWide.channelID, clickLogWide.yearMonth, clickLogWide.count, clickLogWide.isMonthNew)
        )
      }
    }
    // 2. 分组

    val keyedStream: KeyedStream[ChannelPvUv, String] = channelPvUvDS.keyBy {
      channelPvUv => channelPvUv.channelId + channelPvUv.yearMonthDayHour
    }
    // 3. 窗口
    val windowedStream: WindowedStream[ChannelPvUv, String, TimeWindow] = keyedStream.timeWindow(Time.seconds(3))

    // 4. 聚合
    val reduceDataStream: DataStream[ChannelPvUv] = windowedStream.reduce {
      (t1, t2) => ChannelPvUv(t1.channelId, t1.yearMonthDayHour, t1.pv + t2.pv, t1.uv + t2.uv)
    }

    // 5. 落地
    reduceDataStream.addSink(new SinkFunction[ChannelPvUv] {

      override def invoke(value: ChannelPvUv): Unit = {
        val tableName = "channel_pvuv"
        val clfName = "info"
        val channelIdColumn = "channelId"
        val yearMonthDayHourColumn = "yearMonthDayHour"
        val pvColumn = "pv"
        val uvColumn = "uv"

        val rowkey = value.channelId + ":" + value.yearMonthDayHour

        val pvInHBase: String = HBaseUtil.getData(tableName, rowkey, clfName, pvColumn)
        val uvInHBase: String = HBaseUtil.getData(tableName, rowkey, clfName, uvColumn)

        var totalPv = 0L
        var totalUv = 0L

        // 如果HBase中没有pv值,那么就把当前值保存,如果有值,进行累加
        if (StringUtils.isBlank(pvInHBase)) {
          totalPv = value.pv
        } else {
          totalPv = value.pv + pvInHBase.toLong
        }

        // 如果HBase中没有pv值,那么就把当前值保存,如果有值,进行累加
        if (StringUtils.isBlank(uvInHBase)) {
          totalUv = value.uv
        } else {
          totalUv = value.uv + uvInHBase.toLong
        }

        HBaseUtil.putMapData(tableName, rowkey, clfName, Map(

          channelIdColumn -> value.channelId,
          yearMonthDayHourColumn -> value.yearMonthDayHour,
          pvColumn -> totalPv.toString,
          uvColumn -> totalUv.toString
        ))


      }
    })


  }
}
