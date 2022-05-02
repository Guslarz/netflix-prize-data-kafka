package com.kaczmarek.bigdata.mapper

import com.kaczmarek.bigdata.model.MovieRatingResult
import org.slf4j.LoggerFactory

class MovieRatingResultToCsvMapper extends (MovieRatingResult => String) {

    private val logger = LoggerFactory.getLogger(getClass)

    override def apply(value: MovieRatingResult): String = {
        logger.info(">> {}", value)
        val result = Array(
            value.year.toString,
            value.month.toString,
            value.title,
            value.voteCount.toString,
            value.ratingSum.toString,
            value.uniqueVoterCount.toString
        ).mkString(",")
        logger.info("<< {}", result)
        result
    }
}
