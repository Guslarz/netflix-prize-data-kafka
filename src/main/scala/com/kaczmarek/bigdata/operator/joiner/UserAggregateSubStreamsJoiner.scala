package com.kaczmarek.bigdata.operator.joiner

import com.kaczmarek.bigdata.model.MovieRatingUserAggregateValue

class UserAggregateSubStreamsJoiner
    extends ((MovieRatingUserAggregateValue, MovieRatingUserAggregateValue) => MovieRatingUserAggregateValue) {

    override def apply(lhs: MovieRatingUserAggregateValue, rhs: MovieRatingUserAggregateValue):
    MovieRatingUserAggregateValue = MovieRatingUserAggregateValue(
        voteCount = sum(lhs, rhs, _.voteCount),
        ratingSum = sum(lhs, rhs, _.ratingSum)
    )

    private def sum(
        lhs: MovieRatingUserAggregateValue,
        rhs: MovieRatingUserAggregateValue,
        getter: MovieRatingUserAggregateValue => Int): Int =
        Iterable(lhs, rhs)
            .filter(_ != null)
            .map(getter)
            .sum
}
