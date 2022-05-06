package com.kaczmarek.bigdata.operator.mapper

import com.kaczmarek.bigdata.model.{MovieRatingResultKey, MovieRatingResultValue, MovieRatingJoinedResult}
import com.kaczmarek.bigdata.util.DateUtils

class MovieRatingJoinedResultToMovieRatingResultMapper
    extends ((Int, MovieRatingJoinedResult) => (MovieRatingResultKey, MovieRatingResultValue)) {

    override def apply(key: Int, value: MovieRatingJoinedResult):
    (MovieRatingResultKey, MovieRatingResultValue) = (
        MovieRatingResultKey(
            movieId = key,
            yearMonth = DateUtils.formatYearMonth(value.year, value.month)
        ),
        MovieRatingResultValue(
            title = value.title,
            voteCount = value.voteCount,
            ratingSum = value.ratingSum,
            uniqueVoterCount = value.uniqueVoterCount
        )
    )
}
