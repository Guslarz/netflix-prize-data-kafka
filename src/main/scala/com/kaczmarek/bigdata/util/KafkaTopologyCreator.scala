package com.kaczmarek.bigdata.util

import com.kaczmarek.bigdata.model._
import com.kaczmarek.bigdata.operator.accumulator.MovieRatingAggregator
import com.kaczmarek.bigdata.operator.joiner.MovieRatingResultJoiner
import com.kaczmarek.bigdata.operator.mapper.{MovieRatingAggregateToMovieRatingResultWithoutTitleMapper, MovieRatingToMovieRatingUserAggregateMapper, MovieToMovieTitleMapper}
import com.kaczmarek.bigdata.operator.reducer.{MovieRatingUserAggregateReducer, NoOpReducer}
import com.kaczmarek.bigdata.operator.selector.MovieRatingAggregateSelector
import com.kaczmarek.bigdata.parser.{MovieParser, MovieRatingVoteParser}
import com.kaczmarek.bigdata.serde.CustomSerdes
import com.kaczmarek.bigdata.serde.CustomSerdes._
import com.kaczmarek.bigdata.timestamp.MovieRatingVoteTimestampExtractor
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}

object KafkaTopologyCreator {

    val MOVIE_RATING_VOTES_TOPIC: String = "movie-rating-votes"
    val MOVIE_TITLES_TOPIC: String = "movie-titles"
    val ETL_RESULT_STORE: String = "movie-ratings"

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
            .map(new MovieRatingToMovieRatingUserAggregateMapper)
            .groupByKey
            .reduce(new MovieRatingUserAggregateReducer)

        val movieRatingVoteAggregatesTable:
            KTable[MovieRatingAggregateKey, MovieRatingAggregateValue] = movieRatingVoteUserAggregatesTable
            .groupBy(new MovieRatingAggregateSelector)
            .aggregate(MovieRatingAggregator.initializer())(MovieRatingAggregator.adder, MovieRatingAggregator.subtractor)

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
            .join(movieTitlesTable, Materialized.as(ETL_RESULT_STORE)(Serdes.Integer, CustomSerdes.movieRatingResult))(new MovieRatingResultJoiner)

        builder.build()
    }
}
