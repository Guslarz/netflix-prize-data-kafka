package com.kaczmarek.bigdata.parser

import com.kaczmarek.bigdata.model.Movie

class MovieParser extends AbstractParser[String, Movie] {

    def parse(line: String): Movie = {
        val array = line.split(',')
        Movie(
            id = array(0).toInt,
            year = array(1).toInt,
            title = array(2)
        )
    }
}
