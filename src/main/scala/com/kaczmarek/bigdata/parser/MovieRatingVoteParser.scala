package com.kaczmarek.bigdata.parser

import java.text.SimpleDateFormat
import com.kaczmarek.bigdata.model.MovieRatingVote

class MovieRatingVoteParser extends AbstractParser[String, MovieRatingVote] {

    private val FORMAT = "yyyy-MM-dd"
    private val FORMATTER = new SimpleDateFormat(FORMAT)

    def parse(line: String): MovieRatingVote = {
        val array = line.split(',')
        MovieRatingVote(
            date = FORMATTER.parse(array(0)),
            movieId = array(1).toInt,
            userId = array(2).toInt,
            rating = array(3).toInt
        )
    }
}
