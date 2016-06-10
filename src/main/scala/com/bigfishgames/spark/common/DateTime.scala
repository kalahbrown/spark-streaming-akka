package com.bigfishgames.spark.common

import java.text.SimpleDateFormat
import java.util.Calendar

object DateTime {

  def currentTime = {
    val now = Calendar.getInstance.getTime
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd-H.m.s.S")
    dateFormat.format(now)
  }

  def currentDate = {
    val now = Calendar.getInstance.getTime
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    dateFormat.format(now)
  }

}