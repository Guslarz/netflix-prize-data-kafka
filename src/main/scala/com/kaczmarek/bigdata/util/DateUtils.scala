package com.kaczmarek.bigdata.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object DateUtils {

    private val TIMESTAMP_FORMAT = "yyyy-MM-dd"

    def getYear(date: Date): Int = toCalendar(date)
        .get(Calendar.YEAR)

    def getMonth(date: Date): Int = toCalendar(date)
        .get(Calendar.MONTH)

    def formatTimestamp(timestamp: Long): String = new SimpleDateFormat(TIMESTAMP_FORMAT)
        .format(new Date(timestamp))

    private def toCalendar(date: Date): Calendar = new Calendar.Builder()
        .setInstant(date)
        .build()
}
