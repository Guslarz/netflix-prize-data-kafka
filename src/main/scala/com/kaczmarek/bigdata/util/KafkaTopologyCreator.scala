package com.kaczmarek.bigdata.util

import com.kaczmarek.bigdata.model._
import com.kaczmarek.bigdata.operator.filter.AnomalyFilter
import com.kaczmarek.bigdata.operator.joiner.{AnomalyResultJoiner, MovieRatingResultJoiner}
import com.kaczmarek.bigdata.operator.mapper._
import com.kaczmarek.bigdata.operator.reducer.{AnomalyAggregateReducer, MovieRatingReducer, MovieRatingUserAggregateReducer, NoOpReducer}
import com.kaczmarek.bigdata.operator.selector.MovieRatingAggregateSelector
import com.kaczmarek.bigdata.parser.{MovieParser, MovieRatingVoteParser}
import com.kaczmarek.bigdata.serde.CustomSerdes
import com.kaczmarek.bigdata.serde.CustomSerdes._
import com.kaczmarek.bigdata.timestamp.MovieRatingVoteTimestampExtractor
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.{TimeWindows, Windowed}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, Serdes, StreamsBuilder}

import java.time.Duration

object KafkaTopologyCreator {

    val MOVIE_RATING_VOTES_TOPIC: String = "movie-rating-votes"
    val MOVIE_TITLES_TOPIC: String = "movie-titles"
    val ETL_RESULT_STORE: String = "movie-ratings"
    val ANOMALY_RESULT_STORE: String = "popular-movies"

    def createTopology(params: Params): Topology = {
        val builder = new StreamsBuilder
        val movieParser = new MovieParser
        val movieRatingVoteParser = new MovieRatingVoteParser

        val movieRatingVotesStream: KStream[String, MovieRatingVote] = builder
            .stream(MOVIE_RATING_VOTES_TOPIC)(Consumed
                .`with`(Serdes.String, Serdes.String)
                .withTimestampExtractor(new MovieRatingVoteTimestampExtractor))
            .flatMapValues(value => movieRatingVoteParser.tryParse(value).toIterable)

        val movieRatingVoteUserAggregatesTable:
            KTable[MovieRatingUserAggregateKey, MovieRatingUserAggregateValue] = movieRatingVotesStream
            .map(new MovieRatingVoteToMovieRatingUserAggregateMapper)
            .groupByKey
            .reduce(new MovieRatingUserAggregateReducer)

        val movieRatingVoteAggregatesTable:
            KTable[MovieRatingAggregateKey, MovieRatingAggregateValue] = movieRatingVoteUserAggregatesTable
            .groupBy(new MovieRatingAggregateSelector)
            .reduce(MovieRatingReducer.adder, MovieRatingReducer.subtractor)

        val movieRatingResultsWithoutTitleTable: KTable[Int, MovieRatingResultWithoutTitle] =
            movieRatingVoteAggregatesTable
                .groupBy(new MovieRatingAggregateToMovieRatingResultWithoutTitleMapper)
                .reduce(new NoOpReducer[MovieRatingResultWithoutTitle], new NoOpReducer[MovieRatingResultWithoutTitle])

        val moviesStream: KStream[String, Movie] = builder
            .stream[String, String](MOVIE_TITLES_TOPIC)
            .flatMapValues(value => movieParser.tryParse(value).toIterable)

        val movieTitlesTable: KTable[Int, String] = moviesStream
            .map(new MovieToMovieTitleMapper)
            .groupByKey
            .reduce(new NoOpReducer[String])

        movieRatingResultsWithoutTitleTable
            .join(movieTitlesTable, Materialized
                .as[Int, MovieRatingResult, ByteArrayKeyValueStore]
                    (ETL_RESULT_STORE)
                    (Serdes.Integer, CustomSerdes.movieRatingResultJson)
            )(new MovieRatingResultJoiner)

        val anomalyAggregateTable: KTable[Windowed[Int], AnomalyAggregate] = movieRatingVotesStream
            .map(new MovieRatingVoteToAnomalyAggregateMapper)
            .groupByKey
            .windowedBy(TimeWindows
                .of(Duration.ofDays(params.anomalyWindowDuration))
                .advanceBy(Duration.ofDays(1))
                .grace(Duration.ofDays(1)))
            .reduce(new AnomalyAggregateReducer)(Materialized.`with`(Serdes.Integer, CustomSerdes.anomalyAggregate)
                .withRetention(Duration.ofDays(params.anomalyWindowDuration + 1)))

        val anomalyResultsWithoutTitleTable: KStream[Windowed[Int], AnomalyResultWithoutTitle] = anomalyAggregateTable
            .toStream
            .mapValues(new AnomalyAggregateToAnomalyResultWithoutTitleMapper)
            .filter(new AnomalyFilter(params))

        val anomalyJoinedResults: KStream[Int, AnomalyJoinedResult] = anomalyResultsWithoutTitleTable
            .map(new AnomalyResultWithoutTitleToAnomalyJoinableResultMapper)
            .join(movieTitlesTable)(new AnomalyResultJoiner)

        anomalyJoinedResults
            .map(new AnomalyJoinedResultToAnomalyResultMapper)
            .groupByKey
            .reduce(new NoOpReducer[AnomalyResultValue])(Materialized
                .as(ANOMALY_RESULT_STORE)
                (CustomSerdes.anomalyResultKeyJson, CustomSerdes.anomalyResultValueJson)
            )

        builder.build()
    }
}
