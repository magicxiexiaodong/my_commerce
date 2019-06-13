package com.xxd.product

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

/**
  * Created by 38636 on 2019/6/12.
  */
class GroupConcatDistinct extends UserDefinedAggregateFunction {

  // 聚合函数的输入数据结构
  override def inputSchema: StructType = StructType(StructField("cityInfo", StringType) :: Nil)

  // 缓存区数据结构
  override def bufferSchema: StructType = StructType(StructField("bufferCityInfo", StringType) :: Nil)

  // 聚合函数返回值数据结构
  override def dataType: DataType = StringType

  // 聚合函数是否是幂等的，即相同输入是否总是能得到相同输出
  override def deterministic: Boolean = true

  // 初始化缓冲区
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }

  // 给聚合函数传入一条新数据进行处理
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    var bufferCityInfo = buffer.getString(0)
    val cityInfo = input.getString(0)

    if (!bufferCityInfo.contains(cityInfo)) {
      if ("".equals(bufferCityInfo)) {
        bufferCityInfo += cityInfo
      } else {
        bufferCityInfo += "," + cityInfo
      }
      buffer.update(0, bufferCityInfo)
    }
  }
  // 合并聚合函数缓冲区
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var bufferCityInfo1 = buffer1.getString(0)
    val bufferCityInfo2 = buffer2.getString(0)

    for (cityInfo <- bufferCityInfo2.split(",")) {
      if (!bufferCityInfo1.contains(cityInfo)) {
        if ("".equals(bufferCityInfo1)) {
          bufferCityInfo1 += cityInfo
        } else {
          bufferCityInfo1 += "," + cityInfo
        }
      }
    }
    buffer1.update(0, bufferCityInfo1)
  }
  // 计算最终结果
  override def evaluate(buffer: Row): Any = {
    buffer.getString(0)
  }
}
