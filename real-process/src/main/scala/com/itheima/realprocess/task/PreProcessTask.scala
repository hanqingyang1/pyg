package com.itheima.realprocess.task

import com.itheima.realprocess.bean.{ClickLogWide, Message}
import com.itheima.realprocess.util.HBaseUtil
import org.apache.commons.lang.time.FastDateFormat
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._

object PreProcessTask {

  def process(watermarkDataStream:DataStream[Message]): DataStream[ClickLogWide] ={
    val clickWideDataStream: DataStream[ClickLogWide] = watermarkDataStream.map {
      msg =>
        val yearMonth = FastDateFormat.getInstance("yyyyMM").format(msg.timeStamp)
        val yearMonthDay = FastDateFormat.getInstance("yyyyMMdd").format(msg.timeStamp)
        val yearMonthDayHour = FastDateFormat.getInstance("yyyyMMddHH").format(msg.timeStamp)

        val address = msg.clickLog.country + msg.clickLog.province + msg.clickLog.city

        val isNewTuple = isNewProcess(msg)

        ClickLogWide(
          msg.clickLog.channelID,
          msg.clickLog.categoryID,
          msg.clickLog.produceID,
          msg.clickLog.country,
          msg.clickLog.province,
          msg.clickLog.city,
          msg.clickLog.network,
          msg.clickLog.source,
          msg.clickLog.browserType,
          msg.clickLog.entryTime,
          msg.clickLog.leaveTime,
          msg.clickLog.userID,
          msg.count,
          msg.timeStamp,
          address,
          yearMonth,
          yearMonthDay,
          yearMonthDayHour,
          isNewTuple._1,
          isNewTuple._2,
          isNewTuple._3,
          isNewTuple._4
        )
    }
    clickWideDataStream
  }


  private def isNewProcess(msg:Message): (Int,Int,Int,Int) ={
    var isNew = 0
    var isHourNew = 0
    var isDayNew = 0
    var isMonthNew = 0

    val tableName = "user_History"
    val clfName = "info"
    val rowKey = msg.clickLog.userID + msg.clickLog.channelID
    val columnName = "userid"
    // 频道ID(channelid)
    val channelIdColName = "channelId"
    // 最后访问时间（时间戳）(lastVisitedTime)
    val lastVisitedTimeColName = "lastVisitedTime"
    
    val userId = HBaseUtil.getData(tableName,rowKey,clfName,columnName)
    val channelId = HBaseUtil.getData(tableName,rowKey,clfName,channelIdColName)
    val lastVisitedTime = HBaseUtil.getData(tableName,rowKey,clfName,lastVisitedTimeColName)
    if(StringUtils.isBlank(userId)){
      isNew = 1
      isHourNew = 1
      isDayNew = 1
      isMonthNew = 1

      HBaseUtil.putMapData(tableName,rowKey,clfName,Map(
        columnName -> msg.clickLog.userID,
        channelIdColName -> msg.clickLog.channelID,
        lastVisitedTimeColName -> msg.timeStamp.toString
      ))
    }else{
      isNew = 0
      isHourNew = compareData(msg.timeStamp,lastVisitedTime.toLong,"yyyyMMddHH")
      isDayNew = compareData(msg.timeStamp,lastVisitedTime.toLong,"yyyyMMdd")
      isMonthNew = compareData(msg.timeStamp,lastVisitedTime.toLong,"yyyyMM")

      //更新用户时间戳
      HBaseUtil.putData(tableName,rowKey,clfName,lastVisitedTimeColName,msg.timeStamp.toString)

    }

    (isNew,isHourNew,isDayNew,isMonthNew)
  }

  /**
    * 比较时间
    * @param currentTime    当前时间
    * @param historyTime    历史时间
    * @param format         时间格式化方式
    * @return
    *         1 : 当前时间大于历史时间返回
    *         0 : 当前时间小于等于历史时间
    */
  private def compareData(currentTime:Long,historyTime:Long,format:String): Int ={
    val correntTimeStr = timeStamp2Str(currentTime,format)
    val historyTimeStr = timeStamp2Str(historyTime,format)

    var result = correntTimeStr.compareTo(historyTimeStr)
    if(result > 0){
      result = 1
    }else{
      result = 0
    }
    result

  }

  /**
    * 将时间戳格式化为日期时间格式
    *
    * @param timestamp 时间戳
    * @param format    日期时间格式
    */
  private def  timeStamp2Str(timestamp:Long,format:String):String = {
      FastDateFormat.getInstance(format).format(timestamp)
  }
}
