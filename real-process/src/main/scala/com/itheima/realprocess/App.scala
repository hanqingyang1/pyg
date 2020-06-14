package com.itheima.realprocess


import java.lang
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.itheima.realprocess.bean.{ClickLog, ClickLogWide, Message}
import com.itheima.realprocess.task._
import com.itheima.realprocess.util.GlobalConfigUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09

object App {

  def main(args: Array[String]): Unit = {

    // ---------------初始化FLink的流式环境--------------
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置处理时间为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置并行度
    env.setParallelism(1)

    //测试，加载本地数据
//    val source = env.fromCollection(
//      List("hadoop", "hive", "hbase", "flink")
//    )
//    source.print()

    //添加checkpoint支持
    //5秒启动一次checkpoint
    env.enableCheckpointing(5000)
    //设置checkpoint只checkpoint几次
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //设置两次checkpoint最小间隔时间
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)
    //设置checkpoint超时时长
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    //设置最大并行度
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    //设置程序关闭时，启动额外的checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //设置checkpoint保存地址
    env.setStateBackend(new FsStateBackend("hdfs://node01:8020/pyg/flink-checkpoint/"))

    //整合Kafka
    var properties = new Properties()

   //# Kafka集群地址
    properties.setProperty("bootstrap.servers",GlobalConfigUtil.bootstrapServers)
    //# ZooKeeper集群地址
    properties.setProperty("zookeeper.connect",GlobalConfigUtil.zookeeperConnect)
    //# Kafka Topic名称
    properties.setProperty("input.topic",GlobalConfigUtil.inputTopic)
    //# 消费组ID
    properties.setProperty("group.id",GlobalConfigUtil.groupId)
    //# 自动提交拉取到消费端的消息offset到kafka
    properties.setProperty("enable.auto.commit",GlobalConfigUtil.enableAutoCommit)
    //# 自动提交offset到zookeeper的时间间隔单位（毫秒）
    properties.setProperty("auto.commit.interval.ms",GlobalConfigUtil.autoCommitIntervalMs)
    //# 每次消费最新的数据
    properties.setProperty("auto.offset.reset",GlobalConfigUtil.autoOffsetReset)
    //创建Kafka客户端
    val kafkaConsumer = new FlinkKafkaConsumer09[String](GlobalConfigUtil.inputTopic,new SimpleStringSchema(),properties)

    //添加数据源
    val dataStream: DataStream[String] = env.addSource(kafkaConsumer)

    val tuple = dataStream.map { msgJson =>
      val jsonObject = JSON.parseObject(msgJson)
      val message = jsonObject.getString("message")
      val timeStamp = jsonObject.getLong("timeStamp")
      val count = jsonObject.getLong("count")
//      (ClickLog(message), count, timeStamp)
      Message(ClickLog(message), count, timeStamp)
    }

    val watermarkDataStream = tuple.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Message] {

      //记录最大事件时间
      var currentTimeStamp = 0l
      //设置最大延迟时间
      val maxDelayTime = 2000l

      //设置水印时间为最大事件时间减2秒
      override def getCurrentWatermark: Watermark = {
        new Watermark(currentTimeStamp - maxDelayTime)
      }

      //比较两个时间选择最大的一个，如 60 跟 58 比较选择 60的事件时间
      override def extractTimestamp(t: Message, l: Long): Long = {
        currentTimeStamp = Math.max(t.timeStamp, l)
        currentTimeStamp
      }
    })
    val clickLogWideDataStream: DataStream[ClickLogWide] = PreProcessTask.process(watermarkDataStream)

//    clickLogWideDataStream.print()
//      ChannelRealHotTask.process(clickLogWideDataStream)
//      ChannelPvUvTask.process(clickLogWideDataStream)
//    ChannelFreshnessTask.process(clickLogWideDataStream)
//    ChannelAreaTask.process(clickLogWideDataStream)
      ChannelNetWorkTask.process(clickLogWideDataStream)
//    dataStream.print()




    env.execute()


  }
}
