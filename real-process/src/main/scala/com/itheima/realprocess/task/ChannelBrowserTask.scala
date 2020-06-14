package com.itheima.realprocess.task

import com.itheima.realprocess.bean.ClickLogWide
import com.itheima.realprocess.util.HBaseUtil
import org.apache.commons.lang.StringUtils
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.api.scala._

case class ChannelBrowser(
                           var channelId: String,
                           var browser: String,
                           var date: String,
                           var pv: Long,
                           var uv: Long,
                           var newCount: Long,
                           var oldCount: Long
                         )

object ChannelBrowserTask extends BaseTask[ChannelBrowser] {
  override def map(clickLogWideDataStream: DataStream[ClickLogWide]): DataStream[ChannelBrowser] = {


    clickLogWideDataStream.flatMap {
      clickLogWide => {
        List(
          ChannelBrowser(
            clickLogWide.channelID,
            clickLogWide.browserType,
            clickLogWide.yearMonth,
            clickLogWide.count,
            clickLogWide.isMonthNew,
            clickLogWide.isNew,
            isOld(clickLogWide.isNew, clickLogWide.isMonthNew)
          ),
          ChannelBrowser(
            clickLogWide.channelID,
            clickLogWide.browserType,
            clickLogWide.yearMonthDay,
            clickLogWide.count,
            clickLogWide.isDayNew,
            clickLogWide.isNew,
            isOld(clickLogWide.isNew, clickLogWide.isDayNew)
          ),
          ChannelBrowser(
            clickLogWide.channelID,
            clickLogWide.browserType,
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

  override def keyBy(mapDataStream: DataStream[ChannelBrowser]): KeyedStream[ChannelBrowser, String] = {
    mapDataStream.keyBy {
      browser =>
        browser.channelId + ":" + browser.browser + ":" + browser.date
    }
  }

  override def reduce(windowedStream: WindowedStream[ChannelBrowser, String, TimeWindow]): DataStream[ChannelBrowser] = {
    windowedStream.reduce {
      (t1, t2) => {
        ChannelBrowser(t1.channelId, t1.browser, t1.date,
          t1.pv + t2.pv, t1.uv + t2.uv, t1.newCount + t2.newCount, t1.oldCount + t2.oldCount)
      }
    }
  }

  override def sink2HBase(reduceDataStream: DataStream[ChannelBrowser]): Unit = {

    reduceDataStream.addSink(
      browser => {
        // 创建HBase相关列
        tableName = "channel_browser"
        rowkey = s"${browser.channelId}:${browser.browser}:${browser.date}"
        browserColumn = "browser"

        // 查询HBase

        val resultMap: Map[String, String] = HBaseUtil.getMapData(tableName, rowkey, clfName, List(
          pvColumn, uvColumn, newCountColumn, oldCountColumn
        ))


        // 保存数据
        HBaseUtil.putMapData(
          tableName, rowkey, clfName, Map(
            channelIdColumn -> browser.channelId,
            browserColumn -> browser.browser,
            dateColumn -> browser.date,
            pvColumn -> getTotal(resultMap,pvColumn,browser.pv),
            uvColumn -> getTotal(resultMap,uvColumn,browser.uv),
            newCountColumn -> getTotal(resultMap,newCountColumn,browser.newCount),
            oldCountColumn -> getTotal(resultMap,oldCountColumn,browser.oldCount)
          )
        )
      }
    )

  }




}














