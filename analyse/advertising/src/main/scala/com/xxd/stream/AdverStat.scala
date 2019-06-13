package com.xxd.stream

import com.xxd.commos.conf.ConfigurationManager
import com.xxd.commos.constant.Constants
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by 38636 on 2019/6/13.
  */
object AdverStat {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("adver").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    //得到sparkStreaming
    val streamingContext = new StreamingContext(sparkSession.sparkContext,Seconds(5))

    // 得到kafka 的 brokerlist 与 topics
    val kafka_brokers = ConfigurationManager.config.getString(Constants.KAFKA_BROKER_LIST);
    val kafka_topics = ConfigurationManager.config.getString(Constants.KAFKA_TOPICS)

    val kafkaParam = Map(
      "bootstrap.servers" -> kafka_brokers,
      "key.deserailizer" -> classOf[StringDeserializer],
      "value.deserailizer" -> classOf[StringDeserializer],
      "group.id" -> "group1",
      // auto.offset.reset
      // latest: 先去Zookeeper获取offset，如果有，直接使用，如果没有，从最新的数据开始消费；
      // earlist: 先去Zookeeper获取offset，如果有，直接使用，如果没有，从最开始的数据开始消费
      // none: 先去Zookeeper获取offset，如果有，直接使用，如果没有，直接报错
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false:java.lang.Boolean)
    )
    KafkaUtils.createDirectStream(streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(Array(kafka_topics),kafkaParam));
  }
}
