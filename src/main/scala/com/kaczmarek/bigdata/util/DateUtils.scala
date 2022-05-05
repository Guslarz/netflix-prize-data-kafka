package com.kaczmarek.bigdata.util

import java.text.SimpleDateFormat
import java.time.{LocalDate, ZoneId}
import java.util.{Calendar, Date, TimeZone}

object DateUtils {

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


    private def toCalendar(date: Date): Calendar = new Calendar.Builder()
        .setInstant(date)
        .build()

    private def getFormatter(format: String): SimpleDateFormat = {
        val formatter = new SimpleDateFormat(format)
        formatter.setTimeZone(TimeZone.getTimeZone("GMT"))
        formatter
    }
}
