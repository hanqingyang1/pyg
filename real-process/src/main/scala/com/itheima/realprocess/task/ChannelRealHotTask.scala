package com.itheima.realprocess.task

import com.itheima.realprocess.bean.ClickLogWide
import com.itheima.realprocess.util.HBaseUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
case class ChannelRealHot(var channelId:String,var visited:Long)
// 添加一个`ChannelRealHot`样例类，它封装要统计的两个业务字段：频道ID（channelID）、访问数量（visited）
object ChannelRealHotTask {


  /**
    * 点击流日志宽表数据流
    *
    * 分组
    * 划分时间窗口
    * 聚合
    * 落地HBase
    *
    * @param clickLogWideDataStream
    * @return
    */
  def process(clickLogWideDataStream:DataStream[ClickLogWide]): Unit ={

    // 1. 遍历日志宽表,转换为 样例类[ChannelRealHot]
    val channelRealHotStream: DataStream[ChannelRealHot] = clickLogWideDataStream.map {
      clickLogWide =>
        ChannelRealHot(clickLogWide.channelID, clickLogWide.count)
    }

    //分组
    val keyedStream: KeyedStream[ChannelRealHot, String] = channelRealHotStream.keyBy(_.channelId)

    //添加时间窗口
    val timeWindowStream: WindowedStream[ChannelRealHot, String, TimeWindow] = keyedStream.timeWindow(Time.seconds(3))

    //聚合
    val reduceDataStream: DataStream[ChannelRealHot] = timeWindowStream.reduce(
      (t1, t2) =>
        ChannelRealHot(t1.channelId, t1.visited + t1.visited)

    )
    reduceDataStream.addSink(new SinkFunction[ChannelRealHot] {
      override def invoke(value: ChannelRealHot): Unit = {
        var tableName = "channel"
        var columnFamilyName = "info"
        val rowKey = value.channelId
        val channelIdColumn = "channelId"
        val visitedColumn = "visited"

        val visitedValue = HBaseUtil.getData(tableName,rowKey,columnFamilyName,visitedColumn)
        //创建总的临时变量
        var totalVisitedCount = 0l
        if(StringUtils.isBlank(visitedValue)){
          totalVisitedCount = value.visited
        }else{
          totalVisitedCount = visitedValue.toLong + value.visited
        }

        //保存数据到HBase
        HBaseUtil.putMapData(tableName,rowKey,columnFamilyName,Map(
          channelIdColumn -> value.channelId,
          visitedColumn -> totalVisitedCount.toString
        ))
      }
    })

  }
}
