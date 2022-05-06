package com.kaczmarek.bigdata.operator.joiner

import com.kaczmarek.bigdata.model.{MovieRatingJoinedResult, MovieRatingResultWithoutTitle}

class MovieRatingResultJoiner
    extends ((MovieRatingResultWithoutTitle, String) => MovieRatingJoinedResult) {

    override def apply(resultWithoutTitle: MovieRatingResultWithoutTitle, title: String): MovieRatingJoinedResult =
        MovieRatingJoinedResult(
            year = resultWithoutTitle.year,
            month = resultWithoutTitle.month,
            title = title,
            voteCount = resultWithoutTitle.voteCount,
            ratingSum = resultWithoutTitle.ratingSum,
            uniqueVoterCount = resultWithoutTitle.uniqueVoterCount
        )
}
