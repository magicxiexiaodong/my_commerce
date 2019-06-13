package com.xxd.session

import java.util.{Date, Random, UUID}

import com.xxd.commos.conf.ConfigurationManager
import com.xxd.commos.constant.Constants
import com.xxd.commos.model.{UserInfo, UserVisitAction}
import com.xxd.commos.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * 用户访问session统计分析
  *
  * 接收用户创建的分析任务，用户可能指定的条件如下：
  *
  * 1、时间范围：起始日期~结束日期
  * 2、性别：男或女
  * 3、年龄范围
  * 4、职业：多选
  * 5、城市：多选
  * 6、搜索词：多个搜索词，只要某个session中的任何一个action搜索过指定的关键词，那么session就符合条件
  * 7、点击品类：多个品类，只要某个session中的任何一个action点击过某个品类，那么session就符合条件
  *
  * @author Xxd
  *
  */
object UserVisitSessionAnalyze {

  def main(args: Array[String]): Unit = {
    // 得到sparkConf
    val conf = new SparkConf().setAppName("SessionAnalyzer").setMaster("local[*]")

    // 得到sparkSession
    val sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val taskUUID = UUID.randomUUID().toString
    // 获取统计任务参数【为了方便，直接从配置文件中获取，企业中会从一个调度平台获取】
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)

    val taskParam = JSONObject.fromObject(jsonStr)
    //查询 时间 过滤得到actionRDD
    val actionRDD = getActionRDDByDateRange(sparkSession, taskParam);

    val sessionId2ActionRDD = actionRDD.map(item => (item.session_id, item))

    sessionId2ActionRDD.cache()

    val sessionID2GroupRDD = sessionId2ActionRDD.groupByKey()
    sessionID2GroupRDD.cache();

    // 获取聚合数据里面的聚合信息
    val sessionId2FullInfo = getFullInfo(sessionID2GroupRDD, sparkSession)

    // 设置自定义累加器，实现所有数据的统计功能,注意累加器也是懒执行的
    val accumulator = new SessionStatAccumulator

    sparkSession.sparkContext.register(accumulator, "sessionAccumulator")
    // 过滤用户数据
    val sessionId2FilterRDD = getFilteredData(taskParam, sessionId2FullInfo, accumulator)
    // 对数据进行内存缓存
    sessionId2FilterRDD.count() // 此处如果不做foreach 则会报错

    /*for((k,v) <- accumulator.value){
      println("k = " + k + ", value = " + v)
    }*/
    // 获取最终的统计结果
    //getFinalData(sparkSession, taskUUID, accumulator.value)

    // 需求二：session随机抽取
    // sessionId2FilterRDD： RDD[(sid, fullInfo)] 一个session对应一条数据，也就是一个fullInfo
    //sessionRandomExtract(sparkSession, taskUUID, sessionId2FilterRDD)


    // sessionId2ActionRDD: RDD[(sessionId, action)]
    // sessionId2FilterRDD : RDD[(sessionId, FullInfo)]  符合过滤条件的
    // sessionId2FilterActionRDD: join
    // 获取所有符合过滤条件的action数据
    val sessionId2FilterActionRDD = sessionId2ActionRDD.join(sessionId2FilterRDD).map {
      case (sessionId, (action, fullInfo)) =>
        (sessionId, action)
    }

    // 需求三：Top10热门商品统计
    // top10CategoryArray: Array[(sortKey, countInfo)]
    val top10CategoryArray = top10PopularCategories(sparkSession, sessionId2FilterActionRDD, taskUUID)

    /*for( i <- top10CategoryArray){
      println(i)
    }*/

    // 需求四：Top10热门商品的Top10活跃session统计
    // sessionId2FilterActionRDD: RDD[(sessionId, action)]
    // top10CategoryArray: Array[(sortKey, countInfo)]
    top10ActiveSession(sparkSession, taskUUID, sessionId2FilterActionRDD, top10CategoryArray)
    sparkSession.close()
  }


  def top10ActiveSession(sparkSession: SparkSession,
                         taskUUID: String,
                         sessionId2FilterActionRDD: RDD[(String, UserVisitAction)],
                         top10CategoryArray: Array[(SortKey, String)]) = {
    // 2：使用filter
    // cidArray: Array[Long] 包含了Top10热门品类ID
    val cidArray = top10CategoryArray.map {
      case (sortKey, countInfo) =>
        val cid = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        cid
    }

    // 所有符合过滤条件的，并且点击过Top10热门品类的action
    val sessionId2ActionRDD = sessionId2FilterActionRDD.filter {
      case (sessionId, action) =>
        cidArray.contains(action.click_category_id)
    }
    // 按照sessionId进行聚合操作
    val sessionId2GroupRDD = sessionId2ActionRDD.groupByKey()

    // cid2SessionCountRDD: RDD[(cid, sessionCount)]
    val cid2SessionCountRDD = sessionId2GroupRDD.flatMap {
      case (sessionId, iterableAction) =>
        val categoryCountMap = new mutable.HashMap[Long, Long]()
        for (action <- iterableAction) {
          val cid = action.click_category_id
          if (!categoryCountMap.contains(cid))
            categoryCountMap += (cid -> 0)
          categoryCountMap.update(cid, categoryCountMap(cid) + 1)
        }

        for ((cid, count) <- categoryCountMap)
          yield (cid, sessionId + "=" + count)
    }
    //  cid2SessionCountRDD.foreach(println(_))
    val cid2GroupRDD = cid2SessionCountRDD.groupByKey()

    val top10SessionRDD = cid2GroupRDD.flatMap {
      case (cid, iterableSessionCount) =>
        val sortList = iterableSessionCount.toList.sortWith(
          (item1, item2) => {
            item1.split("=")(1).toLong > item2.split("=")(1).toLong
          }
        ).take(10)

        val top10Session = sortList.map {
          case item =>
            val sessionId = item.split("=")(0)
            val count = item.split("=")(1).toLong
            Top10Session(taskUUID, cid, sessionId, count)
        }
        top10Session
    }

    import sparkSession.implicits._
    top10SessionRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_session_0308")
      .mode(SaveMode.Append)
      .save()

  }

  def top10PopularCategories(sparkSession: SparkSession,
                             sessionId2FilterActionRDD: RDD[(String, UserVisitAction)],
                             taskUUID: String) = {
    //获取所有发生过点击、下单、付款的品类
    var cid2CidRDD = sessionId2FilterActionRDD.flatMap {
      case (sessionId, action) =>
        val categoryBuffer = new ArrayBuffer[(Long, Long)]()

        if (action.click_category_id != -1L) {
          categoryBuffer += ((action.click_category_id, action.click_category_id))
        } else if (action.order_category_ids != null) {
          for (orderCid <- action.order_category_ids.split(",")) {
            categoryBuffer += ((orderCid.toLong, orderCid.toLong))
          }
        } else if (action.pay_category_ids != null) {
          for (payCid <- action.pay_category_ids.split(","))
            categoryBuffer += ((payCid.toLong, payCid.toLong))
        }
        categoryBuffer
    }

    cid2CidRDD = cid2CidRDD.distinct()

    // 第二步：统计品类的点击次数
    val cid2ClickCountRDD = getClickCount(sessionId2FilterActionRDD)

    // 获取各个categoryId的下单次数
    val cid2OrderCountRDD = getOrderCount(sessionId2FilterActionRDD)

    //获取各个categoryId的付款次数
    val cid2PayCountRDD = getPayCount(sessionId2FilterActionRDD)

    val cid2FullCountRDD = getFullCount(cid2CidRDD, cid2ClickCountRDD, cid2OrderCountRDD, cid2PayCountRDD)
    val sortKey2FullCountRDD = cid2FullCountRDD.map {
      case (cid, countInfo) =>
        val clickCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT).toLong
        val orderCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT).toLong
        val payCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT).toLong

        val sortKey = SortKey(clickCount, orderCount, payCount)
        (sortKey, countInfo)
    }
    val top10CategoryArray = sortKey2FullCountRDD.sortByKey(false).take(10)
    val top10CategoryRDD = sparkSession.sparkContext.makeRDD(top10CategoryArray).map {
      case (sortKey, countInfo) =>
        val cid = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        val clickCount = sortKey.clickCount
        val orderCount = sortKey.orderCount
        val payCount = sortKey.payCount

        Top10Category(taskUUID, cid, clickCount, orderCount, payCount)
    }
    import sparkSession.implicits._
    top10CategoryRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_category_0308")
      .mode(SaveMode.Append)
      .save

    top10CategoryArray
  }


  def generateRandomIndexList(extractPerDay: Int,
                              dateSessionCount: Long,
                              hourCountMap: mutable.HashMap[String, Long],
                              hourListMap: mutable.HashMap[String, ListBuffer[Int]]) = {
    for ((hour, count) <- hourCountMap) {
      // 获取一个小时要抽取多少个session
      var hourExrCount = ((count / dateSessionCount.toDouble) * extractPerDay).toInt

      // 避免一个小时要抽取的数量超过这个小时的总数
      if (hourExrCount > count) {
        hourExrCount = count.toInt
      }

      val random = new Random()
      hourListMap.get(hour) match {
        case None => hourListMap(hour) = new ListBuffer[Int]
          for (i <- 0 until hourExrCount) {
            var index = random.nextInt(count.toInt)
            while (hourListMap.get(hour).contains(index)) {
              index = random.nextInt(count.toInt)
            }
            hourListMap(hour).append(index)
          }
        case Some(list) =>
          for (i <- 0 until hourExrCount) {
            var index = random.nextInt(count.toInt)
            while (hourListMap.get(hour).contains(index)) {
              index = random.nextInt(count.toInt)
            }
            hourListMap(hour).append(index)
          }
      }
    }
  }

  def sessionRandomExtract(sparkSession: SparkSession, taskUUID: String, sessionId2FilterRDD: RDD[(String, String)]) = {
    val dateHour2FullInfo = sessionId2FilterRDD.map {
      case (sessionId, fullInfo) =>
        val startTime = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
        //startTime=2019-06-10 02:15:14
        val dateHour = DateUtils.getDateHour(startTime)
        (dateHour, fullInfo)
    }

    // hourCountMap: Map[(dateHour, count)]
    val hourCountMap = dateHour2FullInfo.countByKey()

    // dateHourCountMap: Map[(date, Map[(hour, count)])]
    val date2HourCountMap = new mutable.HashMap[String, mutable.HashMap[String, Long]]()

    for ((dateHour, count) <- hourCountMap) {
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)

      date2HourCountMap.get(date) match {
        case None => date2HourCountMap(date) = new mutable.HashMap[String, Long]()
          date2HourCountMap(date) += ((hour, count))
        case Some(map) =>
          date2HourCountMap(date) += ((hour, count))
        //dateHourCountMap(date) += ((hour,count + map(hour)))
      }
    }

    // 解决问题一： 一共有多少天： dateHourCountMap.size
    //              一天抽取多少条：100 / dateHourCountMap.size
    val extractPerDay = 100 / date2HourCountMap.size

    // 解决问题二： 一天有多少session：dateHourCountMap(date).values.sum
    // 解决问题三： 一个小时有多少session：dateHourCountMap(date)(hour)

    // date2HourCountMap: Map[(date, Map[(hour, List)])]
    val date2HourExtractIndexListMap = new mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]]()

    for ((date, hourCountMap) <- date2HourCountMap) {
      val dateSessionCount = hourCountMap.values.sum

      date2HourExtractIndexListMap.get(date) match {
        case None => date2HourExtractIndexListMap(date) = new mutable.HashMap[String, ListBuffer[Int]]()
          generateRandomIndexList(extractPerDay, dateSessionCount, hourCountMap, date2HourExtractIndexListMap(date))
        case Some(map) =>
          generateRandomIndexList(extractPerDay, dateSessionCount, hourCountMap, date2HourExtractIndexListMap(date))
      }
      // 到目前为止，我们获得了每个小时要抽取的session的index

      // 广播大变量，提升任务性能
      val dateHourExtractIndexListMapBd = sparkSession.sparkContext.broadcast(date2HourExtractIndexListMap)

      //抽取数据
      // date2HourCountMap: Map[(date, Map[(hour, List)])]
      // dateHour2FullInfo (dateHour,fullinfo)

      val dateHour2GroupRDD = dateHour2FullInfo.groupByKey()
      // (dateHour,iterable)

      val extractSessionRDD = dateHour2GroupRDD.flatMap {
        case (dateHour, iterableFullInfo) =>
          val date = dateHour.split("_")(0)
          val hour = dateHour.split("_")(1)
          val extractSessionArrayBuffer = new ArrayBuffer[SessionRandomExtract]()
          // 每个小时 要抽去的 list索引
          val extractList = dateHourExtractIndexListMapBd.value.get(date).get(hour)
          var index = 0;

          for (fullInfo <- iterableFullInfo) {
            if (extractList.contains(index)) {
              val sessionId = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SESSION_ID)
              val startTime = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
              val searchKeywords = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
              val clickCategories = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)

              val extractSession = SessionRandomExtract(taskUUID, sessionId, startTime, searchKeywords, clickCategories)
              extractSessionArrayBuffer += extractSession
            }
            index += 1
          }
          extractSessionArrayBuffer
      }
      //extractSessionRDD.foreach(println(_))
      import sparkSession.implicits._
      extractSessionRDD.toDF().write
        .format("jdbc")
        .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
        .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
        .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
        .option("dbtable", "session_extract_0308")
        .mode(SaveMode.Append)
        .save()
    }
  }


  def getFinalData(sparkSession: SparkSession,
                   taskUUID: String,
                   value: mutable.HashMap[String, Int]) = {
    val session_count = value(Constants.SESSION_COUNT).toDouble
    println(session_count)
    // 不同范围访问时长的session个数
    val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    // 不同访问步长的session个数
    val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)

    val stat = SessionAggrStat(taskUUID, session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

    // 写入到 数据库
    val statRDD = sparkSession.sparkContext.makeRDD(Array(stat))
    import sparkSession.implicits._
    statRDD.toDF().write.format("jdbc")
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_ration_0308")
      .mode(SaveMode.Append)
      .save()
  }

  def calculateVisitLength(visitLength: Long, accumulator: SessionStatAccumulator) = {
    if (visitLength >= 1 && visitLength <= 3) {
      accumulator.add(Constants.TIME_PERIOD_1m_3m)
    } else if (visitLength >= 4 && visitLength <= 6) {
      accumulator.add(Constants.TIME_PERIOD_4s_6s)
    } else if (visitLength >= 7 && visitLength <= 9) {
      accumulator.add(Constants.TIME_PERIOD_7s_9s)
    } else if (visitLength >= 10 && visitLength <= 30) {
      accumulator.add(Constants.TIME_PERIOD_10s_30s)
    } else if (visitLength > 30 && visitLength <= 60) {
      accumulator.add(Constants.TIME_PERIOD_30s_60s)
    } else if (visitLength > 60 && visitLength <= 180) {
      accumulator.add(Constants.TIME_PERIOD_1m_3m)
    } else if (visitLength > 180 && visitLength <= 600) {
      accumulator.add(Constants.TIME_PERIOD_3m_10m)
    } else if (visitLength > 600 && visitLength <= 1800) {
      accumulator.add(Constants.TIME_PERIOD_10m_30m)
    } else if (visitLength > 1800) {
      accumulator.add(Constants.TIME_PERIOD_30m)
    }
  }

  def calculateStepLength(stepLength: Long, sessionStatisticAccumulator: SessionStatAccumulator): Unit = {
    if (stepLength >= 1 && stepLength <= 3) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_1_3)
    } else if (stepLength >= 4 && stepLength <= 6) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_4_6)
    } else if (stepLength >= 7 && stepLength <= 9) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_7_9)
    } else if (stepLength >= 10 && stepLength <= 30) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_10_30)
    } else if (stepLength > 30 && stepLength <= 60) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_30_60)
    } else if (stepLength > 60) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_60)
    }
  }

  def getFilteredData(taskParam: JSONObject, sessionId2FullInfo: RDD[(String, String)], accumulator: SessionStatAccumulator) = {

    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    var filterInfo = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
      (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds else "")

    if (filterInfo.endsWith("\\|"))
      filterInfo = filterInfo.substring(0, filterInfo.length - 1)

    val sessionId2FilterRDD = sessionId2FullInfo.filter {
      case (sessionId, fullInfo) =>
        var success = true

        if (!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE))
          success = false;

        if (!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS))
          success = false
        if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES))
          success = false

        if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX))
          success = false

        if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS))
          success = false

        if (!ValidUtils.in(fullInfo, Constants.FIELD_CATEGORY_ID, filterInfo, Constants.PARAM_CATEGORY_IDS))
          success = false

        if (success) {
          accumulator.add(Constants.SESSION_COUNT)
          val visitLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
          val stepLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong
          calculateVisitLength(visitLength, accumulator)
          calculateStepLength(stepLength, accumulator)
        }
        success
    }
    sessionId2FilterRDD
  }

  def getFullInfo(sessionID2GroupRDD: RDD[(String, Iterable[UserVisitAction])], spark: SparkSession) = {
    val userId2AggrInfoRDD = sessionID2GroupRDD.map {
      case (sessionId, iterableAction) =>
        var startTime: Date = null
        var endTime: Date = null

        var userId = -1L
        var stepLength = 0
        val searchKeyWords = new StringBuffer()
        val clickCategories = new StringBuffer()

        for (action <- iterableAction) {
          if (userId == -1L)
            userId = action.user_id

          val actionTime = DateUtils.parseTime(action.action_time);

          if (startTime == null || startTime.after(actionTime))
            startTime = actionTime

          if (endTime == null || startTime.before(actionTime))
            endTime = actionTime

          val searchKeyword = action.search_keyword
          val clickCategory = action.click_category_id

          if (!StringUtils.isEmpty(searchKeyword) && !searchKeyWords.toString.contains(searchKeyword))
            searchKeyWords.append(searchKeyword + ",")

          if (clickCategory != -1L && !clickCategories.toString.contains(clickCategory))
            clickCategories.append(clickCategory + ",")

          stepLength += 1
        }
        // 去除开始和结尾的逗号
        val searchKw = StringUtils.trimComma(searchKeyWords.toString)
        val clickCg = StringUtils.trimComma(clickCategories.toString)

        val visitLength = (endTime.getTime - startTime.getTime) / 1000

        val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKw + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCg + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)
        (userId, aggrInfo)
    }
    val sql = "select * from user_info"
    import spark.implicits._

    val userInfoRDD = spark.sql(sql).as[UserInfo].rdd.map(item => (item.user_id, item))

    val sessionId2FullInfo = userId2AggrInfoRDD.join(userInfoRDD).map {
      case (userId, (aggrInfo, userInfo)) =>
        val sessionId = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID)
        val age = userInfo.age
        val professional = userInfo.professional
        val sex = userInfo.sex
        val city = userInfo.city
        val fullInfo = aggrInfo + "|" + Constants.FIELD_AGE + "=" + age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
          Constants.FIELD_SEX + "=" + sex + "|" +
          Constants.FIELD_CITY + "=" + city
        (sessionId, fullInfo)
    }
    sessionId2FullInfo
  }

  def getActionRDDByDateRange(sparkSession: SparkSession, taskParam: JSONObject) = {

    val startDate = taskParam.get(Constants.PARAM_START_DATE)
    val endDate = taskParam.get(Constants.PARAM_END_DATE)
    val sql = "select * from user_visit_action where date >='" + startDate + "'and date <='" + endDate + "'";

    //从数据库里面查询
    import sparkSession.implicits._

    sparkSession.sql(sql).as[UserVisitAction].rdd
  }


  def getClickCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    val clickFilterRDD = sessionId2FilterActionRDD.filter(item => item._2.click_category_id != -1L)
    val clickNumRDD = clickFilterRDD.map {
      case (session, action) =>
        (action.click_category_id, 1L)
    }
    clickNumRDD.reduceByKey(_ + _)
  }

  def getOrderCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    val orderFilterRDD = sessionId2FilterActionRDD.filter(item => item._2.order_category_ids != null)

    val orderNumRDD = orderFilterRDD.flatMap {
      // action.order_category_ids.split(","): Array[String]
      // action.order_category_ids.split(",").map(item => (item.toLong, 1L)
      // 先将我们的字符串拆分成字符串数组，然后使用map转化数组中的每个元素，
      // 原来我们的每一个元素都是一个string，现在转化为（long, 1L）
      case (sessionId, action) => action.order_category_ids.split(",").
        map(item => (item.toLong, 1L))
    }

    orderNumRDD.reduceByKey(_ + _)
  }

  def getPayCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    val payFilterRDD = sessionId2FilterActionRDD.filter(item => item._2.pay_category_ids != null)

    val payNumRDD = payFilterRDD.flatMap {
      case (sid, action) =>
        action.pay_category_ids.split(",").map(item => (item.toLong, 1L))
    }

    payNumRDD.reduceByKey(_ + _)
  }


  def getFullCount(cid2CidRDD: RDD[(Long, Long)],
                   cid2ClickCountRDD: RDD[(Long, Long)],
                   cid2OrderCountRDD: RDD[(Long, Long)],
                   cid2PayCountRDD: RDD[(Long, Long)]) = {
    val cid2ClickInfoRDD = cid2CidRDD.leftOuterJoin(cid2ClickCountRDD).map {
      case (cid, (categoryId, option)) =>
        val clickCount = if (option.isDefined) option.get else 0
        val aggrCount = Constants.FIELD_CATEGORY_ID + "=" + cid + "|" +
          Constants.FIELD_CLICK_COUNT + "=" + clickCount
        (cid, aggrCount)
    }

    val cid2OrderInfoRDD = cid2ClickInfoRDD.leftOuterJoin(cid2OrderCountRDD).map {
      case (cid, (clickInfo, option)) =>
        val orderCount = if (option.isDefined) option.get else 0
        val aggrInfo = clickInfo + "|" +
          Constants.FIELD_ORDER_COUNT + "=" + orderCount

        (cid, aggrInfo)
    }

    val cid2PayInfoRDD = cid2OrderInfoRDD.leftOuterJoin(cid2PayCountRDD).map {
      case (cid, (orderInfo, option)) =>
        val payCount = if (option.isDefined) option.get else 0
        val aggrInfo = orderInfo + "|" +
          Constants.FIELD_PAY_COUNT + "=" + payCount

        (cid, aggrInfo)
    }
    cid2PayInfoRDD
  }
}
