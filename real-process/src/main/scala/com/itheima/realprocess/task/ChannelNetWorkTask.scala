package com.itheima.realprocess.task

import com.itheima.realprocess.bean.ClickLogWide
import com.itheima.realprocess.util.HBaseUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.api.scala._

case class ChannelNetWork(
                           var channelId: String,
                           var network: String,
                           var date: String,
                           var pv: Long,
                           var uv: Long,
                           var newCount: Long,
                           var oldCount: Long
                         )

object ChannelNetWorkTask extends BaseTask[ChannelNetWork] {
  override def map(clickLogWideDataStream: DataStream[ClickLogWide]): DataStream[ChannelNetWork] = {

    val isOld = (isNew: Int, isDateNew: Int) => if (isNew == 0 && isDateNew == 1) 1 else 0
    clickLogWideDataStream.flatMap {
      clickLogWide => {
        List(
          ChannelNetWork(
            clickLogWide.channelID,
            clickLogWide.network,
            clickLogWide.yearMonth,
            clickLogWide.count,
            clickLogWide.isMonthNew,
            clickLogWide.isNew,
            isOld(clickLogWide.isNew, clickLogWide.isMonthNew)
          ),
          ChannelNetWork(
            clickLogWide.channelID,
            clickLogWide.network,
            clickLogWide.yearMonthDay,
            clickLogWide.count,
            clickLogWide.isDayNew,
            clickLogWide.isNew,
            isOld(clickLogWide.isNew, clickLogWide.isDayNew)
          ),
          ChannelNetWork(
            clickLogWide.channelID,
            clickLogWide.network,
            clickLogWide.yearMonthDayHour,
            clickLogWide.count,
            clickLogWide.isHourNew,
            clickLogWide.isNew,
            isOld(clickLogWide.isNew, clickLogWide.isHourNew)
          )
        )
      }
    }
  }

  override def keyBy(mapDataStream: DataStream[ChannelNetWork]): KeyedStream[ChannelNetWork, String] = {
    mapDataStream.keyBy {
      network =>
        network.channelId + ":" + network.network + ":" + network.date
    }
  }

  override def reduce(windowedStream: WindowedStream[ChannelNetWork, String, TimeWindow]): DataStream[ChannelNetWork] = {
    windowedStream.reduce {
      (t1, t2) => {
        ChannelNetWork(t1.channelId, t1.network, t1.date,
          t1.pv + t2.pv, t1.uv + t2.uv, t1.newCount + t2.newCount, t1.oldCount + t2.oldCount)
      }
    }
  }

  override def sink2HBase(reduceDataStream: DataStream[ChannelNetWork]): Unit = {

    reduceDataStream.addSink(
      network => {
        // 创建HBase相关列
        val tableName = "channel_network"
        val clfName = "info"
        val rowkey = s"${network.channelId}:${network.network}:${network.date}"

        val channelIdColumn = "channelId"
        val networkColumn = "network"
        val dateColumn = "date"
        val pvColumn = "pv"
        val uvColumn = "uv"
        val newCountColumn = "newCount"
        val oldCountColumn = "oldCount"

        // 查询HBase

        val resultMap: Map[String, String] = HBaseUtil.getMapData(tableName, rowkey, clfName, List(
          pvColumn, uvColumn, newCountColumn, oldCountColumn
        ))

        // 数据累加

        var totalPv = 0L
        var totalUv = 0L
        var totalNewCount = 0L
        var totalOldCount = 0L

        // 如果resultMap不为空,并且可以去到相关列的值,那么就进行累加
        if (resultMap != null && StringUtils.isNotBlank(resultMap.getOrElse(pvColumn,""))) {
          totalPv = resultMap(pvColumn).toLong + network.pv
        } else {
          totalPv = network.pv
        }


        if (resultMap != null && StringUtils.isNotBlank(resultMap.getOrElse(uvColumn,""))) {
          totalUv = resultMap(uvColumn).toLong + network.uv
        } else {
          totalUv = network.uv
        }

        if (resultMap != null && StringUtils.isNotBlank(resultMap.getOrElse(newCountColumn,""))) {
          totalNewCount = resultMap(newCountColumn).toLong + network.newCount
        } else {
          totalNewCount = network.newCount
        }

        if (resultMap != null && StringUtils.isNotBlank(resultMap.getOrElse(oldCountColumn,""))) {
          totalOldCount = resultMap(oldCountColumn).toLong + network.oldCount
        } else {
          totalOldCount = network.oldCount
        }


        // 保存数据
        HBaseUtil.putMapData(
          tableName, rowkey, clfName, Map(
            channelIdColumn -> network.channelId,
            networkColumn -> network.network,
            dateColumn -> network.date,
            pvColumn -> totalPv,
            uvColumn -> totalUv,
            newCountColumn -> totalNewCount,
            oldCountColumn -> totalOldCount
          )
        )
      }
    )

  }
}
