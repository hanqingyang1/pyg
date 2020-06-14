package com.itheima.realprocess.task

import com.itheima.realprocess.bean.ClickLogWide
import com.itheima.realprocess.util.HBaseUtil
import org.apache.commons.lang.StringUtils
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow


case class ChannelFreshness(
                             var channelId: String,
                             var date: String,
                             var newCount: Long,
                             val oldCount: Long
                           )

/**
  * 1. 转换
  * 2. 分组
  * 3. 时间窗口
  * 4. 聚合
  * 5. 落地HBase
  */
object ChannelFreshnessTask extends BaseTask [ChannelFreshness]{
  override def map(clickLogWideDataStream: DataStream[ClickLogWide]): DataStream[ChannelFreshness] = {

    // 转换
    val mapDataStream: DataStream[ChannelFreshness] = clickLogWideDataStream.flatMap {
      clickLog =>
        // 如果是老用户 只有在他第一次来的时候 计数为1
        val isOld = (isNew: Int, isDateNew: Int) => if (isNew == 0 && isDateNew == 1) 1 else 0

        List(
          ChannelFreshness(clickLog.channelID, clickLog.yearMonthDayHour, clickLog.isNew, isOld(clickLog.isNew, clickLog.isHourNew)),
          ChannelFreshness(clickLog.channelID, clickLog.yearMonthDay, clickLog.isNew, isOld(clickLog.isNew, clickLog.isDayNew)),
          ChannelFreshness(clickLog.channelID, clickLog.yearMonth, clickLog.isNew, isOld(clickLog.isNew, clickLog.isMonthNew))
        )
    }

    mapDataStream
  }

  override def keyBy(mapDataStream: DataStream[ChannelFreshness]): KeyedStream[ChannelFreshness, String] = {
    // 分组
    mapDataStream.keyBy {
      freshness => (freshness.channelId + freshness.date)
    }
  }

  override def timeWindow(keyedStream: KeyedStream[ChannelFreshness, String]): WindowedStream[ChannelFreshness, String, TimeWindow] = {
    // 时间窗口
    keyedStream.timeWindow(Time.seconds(3))

  }

  override def reduce(windowedStream: WindowedStream[ChannelFreshness, String, TimeWindow]): DataStream[ChannelFreshness] = {
    // 聚合
    windowedStream.reduce {
      (t1, t2) =>
        ChannelFreshness(t1.channelId, t1.date, t1.newCount + t2.newCount, t1.oldCount + t2.oldCount)
    }
  }

  override def sink2HBase(reduceDataStream: DataStream[ChannelFreshness]): Unit = {
    // 落地
    reduceDataStream.addSink(new SinkFunction[ChannelFreshness] {
      override def invoke(value: ChannelFreshness): Unit = {
        // 创建HBase相关变量
        val tableName = "channel_freshness"
        val clfName = "info"
        val rowkey = value.channelId + ":" + value.date

        val channelIdColumn = "channelId"
        val dateColumn = "date"
        val newCountColumn = "newCount"
        val oldCountColumn = "oldCount"


        // 查询历史数据
        val resultMap: Map[String, String] = HBaseUtil.getMapData(tableName, rowkey, clfName, List(newCountColumn, oldCountColumn))

        // 累加

        var totalNewCount = 0L
        var totalOldCount = 0L

        if (resultMap != null && StringUtils.isNotBlank(resultMap.getOrElse(newCountColumn, ""))) {
          totalNewCount = resultMap(newCountColumn).toLong + value.newCount
        } else {
          totalNewCount = value.newCount
        }

        if (resultMap != null && StringUtils.isNotBlank(resultMap.getOrElse(oldCountColumn, ""))) {
          totalOldCount = resultMap(oldCountColumn).toLong + value.oldCount
        } else {
          totalOldCount = value.oldCount
        }

        // 保存数据

        HBaseUtil.putMapData(tableName, rowkey, clfName, Map(
          channelIdColumn -> value.channelId,
          dateColumn -> value.date,
          newCountColumn -> totalNewCount,
          oldCountColumn -> totalOldCount
        ))
      }
    })
  }

}
