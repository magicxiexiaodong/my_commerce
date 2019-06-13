package com.xxd.session

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * Created by 38636 on 2019/6/11.
  */
class SessionStatAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]] {
  val countMap = new mutable.HashMap[String, Int]()

  override def isZero: Boolean = countMap.isEmpty

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val acc = new SessionStatAccumulator
    // scala 中的map  没有 addAll 方法
    acc.countMap ++= this.countMap
    acc
  }

  override def reset(): Unit = countMap.clear()

  override def add(v: String): Unit = {
    countMap.get(v) match {
      case None => countMap += ((v, 1))
      case Some(a) => countMap += ((v, a + 1))
    }
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match {
      case o: AccumulatorV2[String, mutable.HashMap[String, Int]] => {
        for ((k, v) <- o.value) {
          countMap.get(k) match {
            case None => countMap += ((k, 1))
            case Some(a) => countMap += ((k, a + v))
          }
        }
      }
    }
  }

  override def value: mutable.HashMap[String, Int] = this.countMap
}
