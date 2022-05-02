package com.kaczmarek.bigdata.operator.mapper

import com.kaczmarek.bigdata.model.Movie
import org.slf4j.LoggerFactory

class MovieToMovieTitleMapper extends ((String, Movie) => (Int, String)) {

    private val logger = LoggerFactory.getLogger(getClass)

    override def apply(key: String, value: Movie): (Int, String) = {
        logger.info(">> {} {}", key, value: Any)
        val result = (
            value.id,
            value.title
        )
        logger.info("<< {}", result)
        result
    }
}
