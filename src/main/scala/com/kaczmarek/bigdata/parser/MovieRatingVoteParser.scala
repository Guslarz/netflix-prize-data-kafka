package com.kaczmarek.bigdata.parser

import java.text.SimpleDateFormat
import com.kaczmarek.bigdata.model.MovieRatingVote
import com.kaczmarek.bigdata.util.DateUtils

class MovieRatingVoteParser extends AbstractParser[String, MovieRatingVote] {

    def parse(line: String): MovieRatingVote = {
        val array = line.split(',')
        MovieRatingVote(
            date = DateUtils.parseDate(array(0)),
            movieId = array(1).toInt,
            userId = array(2).toInt,
            rating = array(3).toInt
        )
    }
}
