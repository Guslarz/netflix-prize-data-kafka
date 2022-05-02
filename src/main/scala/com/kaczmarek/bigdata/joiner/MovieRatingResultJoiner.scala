package com.kaczmarek.bigdata.joiner

import com.kaczmarek.bigdata.model.{MovieRatingResult, MovieRatingResultWithoutTitle}

class MovieRatingResultJoiner
    extends ((MovieRatingResultWithoutTitle, String) => MovieRatingResult) {

    override def apply(resultWithoutTitle: MovieRatingResultWithoutTitle, title: String): MovieRatingResult =
        MovieRatingResult(
            year = resultWithoutTitle.year,
            month = resultWithoutTitle.month,
            title = title,
            voteCount = resultWithoutTitle.voteCount,
            ratingSum = resultWithoutTitle.ratingSum,
            uniqueVoterCount = resultWithoutTitle.uniqueVoterCount
        )
}
