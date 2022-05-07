package com.kaczmarek.bigdata.util

import java.text.SimpleDateFormat
import java.time.{LocalDate, ZoneId}
import java.util.{Calendar, Date, TimeZone}

object DateUtils {

    private val TIME_ZONE = TimeZone.getTimeZone("GMT")
    private val FORMAT = "yyyy-MM-dd"
    private val FORMATTER = getFormatter(FORMAT)

    def getYear(date: Date): Int = toCalendar(date)
        .get(Calendar.YEAR)

    def getMonth(date: Date): Int = toCalendar(date)
        .get(Calendar.MONTH)

    def formatTimestamp(timestamp: Long): String = FORMATTER
        .format(new Date(timestamp))

    def parseDate(date: String): Date = FORMATTER
        .parse(date)

    def formatYearMonth(year: Int, month: Int): String = s"$year-${monthToString(month)}"

    def yearMonthToTimestamp(year: Int, month: Int): Long = new Calendar.Builder()
        .set(Calendar.YEAR, year)
        .set(Calendar.MONTH, month)
        .set(Calendar.DATE, 1)
        .setTimeOfDay(0, 0, 0)
        .setTimeZone(TIME_ZONE)
        .build()
        .getTimeInMillis

    private def toCalendar(date: Date): Calendar = new Calendar.Builder()
        .setInstant(date)
        .build()

    private def getFormatter(format: String): SimpleDateFormat = {
        val formatter = new SimpleDateFormat(format)
        formatter.setTimeZone(TIME_ZONE)
        formatter
    }

    private def monthToString(month: Int): String = {
        if (month < 10) "0" + (month + 1).toString
        else (month + 1).toString
    }
}
