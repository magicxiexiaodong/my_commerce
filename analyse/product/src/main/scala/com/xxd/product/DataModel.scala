package com.xxd.product

/*
 * Copyright (c) 2017. Atguigu Inc. All Rights Reserved.
 * Date: 10/28/17 12:38 PM.
 * Author: wuyufei.
 */


case class CityClickProduct(city_id:Long,
                                click_product_id:Long)

case class CityAreaInfo(city_id:Long,
                          city_name:String,
                          area:String)

//***************** 输出表 *********************

/**
  *
  * @param taskid 主键
  * @param area  区域
  * @param areaLevel  区域等级
  * @param productid  商品Id
  * @param cityInfos  城市信息
  * @param clickCount  点击次数
  * @param productName  商品名称
  * @param productStatus 商品额外信息 是否自营
  */
case class AreaTop3Product(taskid:String,
                           area:String,
                           areaLevel:String,
                           productid:Long,
                           cityInfos:String,
                           clickCount:Long,
                           productName:String,
                           productStatus:String)