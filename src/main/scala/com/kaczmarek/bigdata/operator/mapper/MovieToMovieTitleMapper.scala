package com.kaczmarek.bigdata.operator.mapper

import com.kaczmarek.bigdata.model.Movie

class MovieToMovieTitleMapper extends ((String, Movie) => (Int, String)) {

    override def apply(key: String, value: Movie): (Int, String) = (
        value.id,
        value.title
    )
}
