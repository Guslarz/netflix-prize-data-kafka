package com.kaczmarek.bigdata.util

import com.kaczmarek.bigdata.model._
import com.kaczmarek.bigdata.operator.filter.AnomalyFilter
import com.kaczmarek.bigdata.operator.joiner.{AnomalyResultJoiner, MovieRatingResultJoiner}
import com.kaczmarek.bigdata.operator.mapper._
import com.kaczmarek.bigdata.operator.reducer.{AnomalyAggregateReducer, MovieRatingReducer, MovieRatingUserAggregateReducer, NoOpReducer}
import com.kaczmarek.bigdata.operator.selector.MovieRatingAggregateSelector
import com.kaczmarek.bigdata.serde.CustomSerdes
import com.kaczmarek.bigdata.serde.CustomSerdes._
import com.kaczmarek.bigdata.timestamp.MovieRatingVoteTimestampExtractor
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.{TimeWindows, Windowed}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}

import java.time.Duration

object KafkaTopologyCreator {

    val MOVIE_RATING_VOTES_TOPIC: String = "movie-rating-votes"
    val MOVIE_TITLES_TOPIC: String = "movie-titles"
    val ETL_RESULT_TOPIC: String = "movie-ratings"
    val ANOMALY_RESULT_TOPIC: String = "popular-movies"

    def createTopology(params: Params): Topology = {
        val builder = new StreamsBuilder

        val movieRatingVotesStream: KStream[String, MovieRatingVote] = builder
            .stream(MOVIE_RATING_VOTES_TOPIC)(Consumed
                .`with`(Serdes.String, CustomSerdes.movieRatingVoteInput)
                .withTimestampExtractor(new MovieRatingVoteTimestampExtractor))

        val movieRatingVoteUserAggregatesTable: KTable[MovieRatingUserAggregateKey, MovieRatingUserAggregateValue] =
            movieRatingVotesStream
                .map(new MovieRatingVoteToMovieRatingUserAggregateMapper)
                .groupByKey
                .reduce(new MovieRatingUserAggregateReducer)

        val movieRatingVoteAggregatesTable: KTable[MovieRatingAggregateKey, MovieRatingAggregateValue] =
            movieRatingVoteUserAggregatesTable
                .groupBy(new MovieRatingAggregateSelector)
                .reduce(MovieRatingReducer.adder, MovieRatingReducer.subtractor)

        val moviesStream: KStream[String, Movie] = builder
            .stream(MOVIE_TITLES_TOPIC)(Consumed
                .`with`(Serdes.String, CustomSerdes.movieInput))

        val movieTitlesTable: KTable[Int, String] = moviesStream
            .map(new MovieToMovieTitleMapper)
            .groupByKey
            .reduce(new NoOpReducer[String])

        val movieRatingResultsWithoutTitleStream: KStream[Int, MovieRatingResultWithoutTitle] =
            movieRatingVoteAggregatesTable
                .toStream
                .map(new MovieRatingAggregateToMovieRatingResultWithoutTitleMapper)

        val movieRatingJoinedResultsStream: KStream[Int, MovieRatingJoinedResult] = movieRatingResultsWithoutTitleStream
            .join(movieTitlesTable)(new MovieRatingResultJoiner)

        movieRatingJoinedResultsStream
            .map(new MovieRatingJoinedResultToMovieRatingResultMapper)
            .to(ETL_RESULT_TOPIC)(Produced.
                `with`(CustomSerdes.movieRatingResultKeyJson, CustomSerdes.movieRatingResultValueJson))

        val anomalyAggregateTable: KTable[Windowed[Int], AnomalyAggregate] = movieRatingVotesStream
            .map(new MovieRatingVoteToAnomalyAggregateMapper)
            .groupByKey
            .windowedBy(TimeWindows
                .of(Duration.ofDays(params.anomalyWindowDuration))
                .advanceBy(Duration.ofDays(1))
                .grace(Duration.ofDays(1)))
            .reduce(new AnomalyAggregateReducer)(Materialized.`with`(Serdes.Integer, CustomSerdes.anomalyAggregate)
                .withRetention(Duration.ofDays(params.anomalyWindowDuration + 1)))

        val anomalyResultsWithoutTitleStream: KStream[Windowed[Int], AnomalyResultWithoutTitle] = anomalyAggregateTable
            .toStream
            .mapValues(new AnomalyAggregateToAnomalyResultWithoutTitleMapper)
            .filter(new AnomalyFilter(params))

        val anomalyJoinedResults: KStream[Int, AnomalyJoinedResult] = anomalyResultsWithoutTitleStream
            .map(new AnomalyResultWithoutTitleToAnomalyJoinableResultMapper)
            .join(movieTitlesTable)(new AnomalyResultJoiner)

        anomalyJoinedResults
            .map(new AnomalyJoinedResultToAnomalyResultMapper)
            .to(ANOMALY_RESULT_TOPIC)(Produced
                .`with`(CustomSerdes.anomalyResultKeyJson, CustomSerdes.anomalyResultValueJson))

        builder.build()
    }
}
