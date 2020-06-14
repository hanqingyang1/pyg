package com.itheima.realprocess.task

import com.itheima.realprocess.bean.ClickLogWide
import com.itheima.realprocess.util.HBaseUtil
import org.apache.commons.lang.StringUtils
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

case class ChannelArea(
                        var channelId: String,
                        var area: String,
                        var date: String,
                        var pv: Long,
                        var uv: Long,
                        var newCount: Long,
                        var oldCount: Long
                      )

object ChannelAreaTask extends BaseTask[ChannelArea] {
  override def map(clickLogWideDataStream: DataStream[ClickLogWide]): DataStream[ChannelArea] = {
    val isOld = (isNew: Int, isDateNew: Int) => if(isNew == 0 && isDateNew == 1) 1 else 0
    clickLogWideDataStream.flatMap {
      clickLogWide =>
        List(
          ChannelArea(clickLogWide.channelID,
            clickLogWide.address,
            clickLogWide.yearMonth,
            clickLogWide.count,
            clickLogWide.isMonthNew,
            clickLogWide.isNew,
            isOld(clickLogWide.isNew, clickLogWide.isMonthNew)
          ),
          ChannelArea(clickLogWide.channelID,
            clickLogWide.address,
            clickLogWide.yearMonthDay,
            clickLogWide.count,
            clickLogWide.isDayNew,
            clickLogWide.isNew,
            isOld(clickLogWide.isNew, clickLogWide.isDayNew)
          ),
          ChannelArea(clickLogWide.channelID,
            clickLogWide.address,
            clickLogWide.yearMonthDayHour,
            clickLogWide.count,
            clickLogWide.isHourNew,
            clickLogWide.isNew,
            isOld(clickLogWide.isNew, clickLogWide.isHourNew)
          )
        )

    }
  }

  override def keyBy(mapDataStream: DataStream[ChannelArea]): KeyedStream[ChannelArea, String] = {
    mapDataStream.keyBy {
      area => (area.channelId + ":" + area.area + ":" + area.date)
    }
  }


  override def reduce(windowedStream: WindowedStream[ChannelArea, String, TimeWindow]) = {
    windowedStream.reduce {
      (t1, t2) =>
        ChannelArea(t1.channelId, t1.area,
          t1.date,
          t1.pv + t2.pv,
          t1.uv + t2.uv,
          t1.newCount + t2.newCount,
          t1.oldCount + t2.oldCount)
    }
  }

  override def sink2HBase(reduceDataStream: DataStream[ChannelArea]): Unit = {

    reduceDataStream.addSink{
      area:ChannelArea=>{
        // HBase相关字段
        val tableName = "channel_area"
        val clfName = "info"
        val rowkey = area.channelId + ":" + area.area + ":" + area.date

        val channelIdColumn = "channelId"
        val areaColumn = "area"
        val dateColumn = "date"
        val pvColumn = "pv"
        val uvColumn = "uv"
        val newCountColumn = "newCount"
        val oldCountColumn = "oldCount"

        // 查询HBase

        val pvInHbase: String = HBaseUtil.getData(tableName,rowkey,clfName,pvColumn)
        val uvInHbase: String = HBaseUtil.getData(tableName,rowkey,clfName,uvColumn)
        val newCountInHbase: String = HBaseUtil.getData(tableName,rowkey,clfName,newCountColumn)
        val oldCountInHbase: String = HBaseUtil.getData(tableName,rowkey,clfName,oldCountColumn)

        // 累加
        var totalPv = 0L
        var totalUv = 0L
        var totalNewCount = 0L
        var totalOldCount = 0L


        if(StringUtils.isNotBlank(pvInHbase)){
          totalPv  = pvInHbase.toLong+area.pv
        }else{
          totalPv = area.pv
        }

        if(StringUtils.isNotBlank(uvInHbase)){
          totalUv  = uvInHbase.toLong+area.uv
        }else{
          totalUv = area.uv
        }

        if(StringUtils.isNotBlank(newCountInHbase)){
          totalNewCount  = newCountInHbase.toLong+area.newCount
        }else{
          totalNewCount = area.newCount
        }

        if(StringUtils.isNotBlank(oldCountInHbase)){
          totalOldCount  = oldCountInHbase.toLong+area.oldCount
        }else{
          totalOldCount = area.oldCount
        }

        // 保存数据

        HBaseUtil.putMapData(tableName,rowkey,clfName,Map(
            channelIdColumn->area.channelId,
            areaColumn->area.area,
            dateColumn->area.date,
            pvColumn->totalPv,
            uvColumn->totalUv,
            newCountColumn->totalNewCount,
            oldCountColumn->totalOldCount
        ))
      }
    }

  }
}
