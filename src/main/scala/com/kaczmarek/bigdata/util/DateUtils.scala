package com.kaczmarek.bigdata.util

import java.util.{Calendar, Date}

object DateUtils {

    def getYear(date: Date): Int = toCalendar(date)
        .get(Calendar.YEAR)

    def getMonth(date: Date): Int = toCalendar(date)
        .get(Calendar.MONTH)

    private def toCalendar(date: Date): Calendar = new Calendar.Builder()
        .setInstant(date)
        .build()
}
