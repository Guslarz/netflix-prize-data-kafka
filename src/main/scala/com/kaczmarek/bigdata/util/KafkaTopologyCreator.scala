package com.kaczmarek.bigdata.util

import com.kaczmarek.bigdata.model._
import com.kaczmarek.bigdata.operator.filter.AnomalyFilter
import com.kaczmarek.bigdata.operator.joiner.{AnomalyResultJoiner, MovieRatingResultJoiner}
import com.kaczmarek.bigdata.operator.mapper._
import com.kaczmarek.bigdata.operator.predicate.{CurrentMovieRatingUserAggregatePredicate, TruePredicate}
import com.kaczmarek.bigdata.operator.reducer.{AnomalyAggregateReducer, MovieRatingReducer, MovieRatingUserAggregateReducer, NoOpReducer}
import com.kaczmarek.bigdata.operator.selector.MovieRatingAggregateSelector
import com.kaczmarek.bigdata.operator.transformer.EventTimestampTransformerSupplier
import com.kaczmarek.bigdata.serde.CustomSerdes
import com.kaczmarek.bigdata.serde.CustomSerdes._
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

    val MAPPED_MOVIE_RATING_VOTES_TOPIC: String = "mapped-movie-rating-votes"

    def createTopology(params: Params): Topology = {
        val builder = new StreamsBuilder

        val movieRatingVotesStream: KStream[String, MovieRatingVote] = readMovieRatingVotesStream(builder)

        val movieRatingVoteAggregatesTable: KTable[MovieRatingAggregateKey, MovieRatingAggregateValue] =
            aggregateMovieRatingVotesToTable(movieRatingVotesStream)

        val movieTitlesTable: KTable[Int, String] = readMovieTitlesTable(builder)

        val movieRatingResultsStream: KStream[MovieRatingResultKey, MovieRatingResultValue] =
            prepareMovieRatingResultsStream(movieRatingVoteAggregatesTable, movieTitlesTable)

        movieRatingResultsStream.to(ETL_RESULT_TOPIC)(Produced.
            `with`(CustomSerdes.movieRatingResultKeyJson, CustomSerdes.movieRatingResultValueJson))

        val eventTimestampedMovieRatingVotesStream: KStream[String, MovieRatingVote] =
            assignEventTimestampToMovieRatingVote(movieRatingVotesStream)

        val anomalyResultsStream: KStream[AnomalyResultKey, AnomalyResultValue] =
            prepareAnomalyResultsStream(params, movieTitlesTable, eventTimestampedMovieRatingVotesStream)

        anomalyResultsStream.to(ANOMALY_RESULT_TOPIC)(Produced
            .`with`(CustomSerdes.anomalyResultKeyJson, CustomSerdes.anomalyResultValueJson))

        builder.build()
    }

    private def readMovieRatingVotesStream(builder: StreamsBuilder): KStream[String, MovieRatingVote] = {
        val movieRatingVotesStream: KStream[String, MovieRatingVote] = builder
            .stream(MOVIE_RATING_VOTES_TOPIC)(Consumed
                .`with`(Serdes.String, CustomSerdes.movieRatingVoteInput))

        movieRatingVotesStream
    }

    private def readMovieTitlesTable(builder: StreamsBuilder): KTable[Int, String] = {
        val moviesStream: KStream[String, Movie] = builder
            .stream(MOVIE_TITLES_TOPIC)(Consumed
                .`with`(Serdes.String, CustomSerdes.movieInput))

        val movieTitlesTable: KTable[Int, String] = moviesStream
            .map(new MovieToMovieTitleMapper)
            .groupByKey
            .reduce(new NoOpReducer[String])

        movieTitlesTable
    }

    private def aggregateMovieRatingVotesToTable(movieRatingVotesStream: KStream[String, MovieRatingVote]):
    KTable[MovieRatingAggregateKey, MovieRatingAggregateValue] = {

        val movieRatingUserAggregatesStream: KStream[MovieRatingUserAggregateKey, MovieRatingUserAggregateValue] =
            movieRatingVotesStream
                .map(new MovieRatingVoteToMovieRatingUserAggregateMapper)

        val movieRatingUserAggregatesSubStreams:
            Array[KStream[MovieRatingUserAggregateKey, MovieRatingUserAggregateValue]] =
            movieRatingUserAggregatesStream
                .branch(
                    new CurrentMovieRatingUserAggregatePredicate,
                    new TruePredicate[MovieRatingUserAggregateKey, MovieRatingUserAggregateValue]
                )
                .zip(List(
                    Duration.ofHours(1),
                    Duration.ofSeconds(10),
                ))
                .map(pair => aggregateMovieRatingUserAggregateSubStream(pair._1, pair._2))

        val movieRatingUserAggregatesMergedTable: KTable[MovieRatingUserAggregateKey, MovieRatingUserAggregateValue] =
            movieRatingUserAggregatesSubStreams(0)
                .merge(movieRatingUserAggregatesSubStreams(1))
                .groupByKey
                .reduce(new NoOpReducer[MovieRatingUserAggregateValue])

        val movieRatingVoteAggregatesTable: KTable[MovieRatingAggregateKey, MovieRatingAggregateValue] =
            movieRatingUserAggregatesMergedTable
                .groupBy(new MovieRatingAggregateSelector)
                .reduce(MovieRatingReducer.adder, MovieRatingReducer.subtractor)

        movieRatingVoteAggregatesTable
    }

    private def aggregateMovieRatingUserAggregateSubStream(
        movieRatingUserAggregatesSubStream: KStream[MovieRatingUserAggregateKey, MovieRatingUserAggregateValue],
        windowDuration: Duration
    ): KStream[MovieRatingUserAggregateKey, MovieRatingUserAggregateValue] = {

        val movieRatingVoteUserAggregatesSubStream: KStream[MovieRatingUserAggregateKey, MovieRatingUserAggregateValue] =
            movieRatingUserAggregatesSubStream
                .groupByKey
                .windowedBy(TimeWindows.of(windowDuration))
                .reduce(new MovieRatingUserAggregateReducer)
                .suppress(Suppressed
                    .untilTimeLimit[Windowed[MovieRatingUserAggregateKey]](
                        windowDuration, Suppressed.BufferConfig.unbounded()))
                .toStream
                .map(new UnwindowKeyMapper[MovieRatingUserAggregateKey, MovieRatingUserAggregateValue])

        movieRatingVoteUserAggregatesSubStream
    }

    private def prepareMovieRatingResultsStream(
        movieRatingVoteAggregatesTable: KTable[MovieRatingAggregateKey, MovieRatingAggregateValue],
        movieTitlesTable: KTable[Int, String]
    ): KStream[MovieRatingResultKey, MovieRatingResultValue] = {

        val movieRatingResultsWithoutTitleStream: KStream[Int, MovieRatingResultWithoutTitle] =
            movieRatingVoteAggregatesTable
                .toStream
                .map(new MovieRatingAggregateToMovieRatingResultWithoutTitleMapper)

        val movieRatingJoinedResultsStream: KStream[Int, MovieRatingJoinedResult] = movieRatingResultsWithoutTitleStream
            .join(movieTitlesTable)(new MovieRatingResultJoiner)

        val movieRatingResultsStream: KStream[MovieRatingResultKey, MovieRatingResultValue] =
            movieRatingJoinedResultsStream
                .map(new MovieRatingJoinedResultToMovieRatingResultMapper)

        movieRatingResultsStream
    }

    private def assignEventTimestampToMovieRatingVote(movieRatingVotesStream: KStream[String, MovieRatingVote]) = {
        val eventTimestampedMovieRatingVotesStream: KStream[String, MovieRatingVote] = movieRatingVotesStream
            .transform(new EventTimestampTransformerSupplier)

        eventTimestampedMovieRatingVotesStream
    }

    private def prepareAnomalyResultsStream(
        params: Params, movieTitlesTable: KTable[Int, String],
        eventTimestampedMovieRatingVotesStream: KStream[String, MovieRatingVote]
    ): KStream[AnomalyResultKey, AnomalyResultValue] = {

        val anomalyAggregateTable: KTable[Windowed[Int], AnomalyAggregate] = eventTimestampedMovieRatingVotesStream
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

        val anomalyJoinedResultsStream: KStream[Int, AnomalyJoinedResult] = anomalyResultsWithoutTitleStream
            .map(new AnomalyResultWithoutTitleToAnomalyJoinableResultMapper)
            .join(movieTitlesTable)(new AnomalyResultJoiner)

        val anomalyResultsStream: KStream[AnomalyResultKey, AnomalyResultValue] = anomalyJoinedResultsStream
            .map(new AnomalyJoinedResultToAnomalyResultMapper)

        anomalyResultsStream
    }
}
