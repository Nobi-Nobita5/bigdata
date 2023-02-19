package com.atguigu.gmall.realtime.util

import java.text.SimpleDateFormat
import java.time.{LocalDate, ZoneId}

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall.realtime.bean.{DauInfo, PageLog}

/**
 * @Author: Xionghx
 * @Date: 2023/02/19/20:43
 * @Version: 1.0
 */
object TimeUtils_Test {
  def main(args: Array[String]): Unit = {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2023-02-20 00:00:00").getTime / 1000)
    println(tomorrowMidnightTimestamp)
  }

  def tomorrowMidnightTimestamp: Long = {
    val zoneId = ZoneId.of("Asia/Shanghai")
    val tomorrow = LocalDate.now(zoneId).plusDays(1).atStartOfDay().atZone(zoneId)
    val tomorrowMidnight = tomorrow.withHour(0).withMinute(0).withSecond(0).withNano(0)
    tomorrowMidnight.toEpochSecond
  }
}
