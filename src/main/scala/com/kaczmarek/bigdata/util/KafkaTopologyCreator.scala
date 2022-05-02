package com.kaczmarek.bigdata.util

import com.kaczmarek.bigdata.joiner.MovieRatingResultJoiner
import com.kaczmarek.bigdata.mapper._
import com.kaczmarek.bigdata.model._
import com.kaczmarek.bigdata.parser.{MovieParser, MovieRatingVoteParser}
import com.kaczmarek.bigdata.reducer.{MovieRatingAggregateReducer, MovieRatingUserAggregateReducer, NoOpReducer}
import com.kaczmarek.bigdata.timestamp.VoteTimestampExtractor
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import com.kaczmarek.bigdata.serde.CustomSerdes._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}

object KafkaTopologyCreator {

    val MOVIE_RATING_VOTES_TOPIC: String = "movie-rating-votes"
    val MOVIE_TITLES_TOPIC: String = "movie-titles"
    val ETL_RESULT_TOPIC: String = "movie-ratings"

    def createTopology(params: Params): Topology = {
        val builder = new StreamsBuilder
        val movieParser = new MovieParser
        val movieRatingVoteParser = new MovieRatingVoteParser

        val movieRatingVotesStream: KStream[String, MovieRatingVote] = builder
            .stream[String, String](MOVIE_RATING_VOTES_TOPIC)(Consumed
                .`with`(Serdes.String, Serdes.String)
                .withTimestampExtractor(new VoteTimestampExtractor))
            .flatMapValues(value => movieRatingVoteParser.tryParse(value).toIterable)

        val movieRatingVoteUserAggregatesTable:
            KTable[MovieRatingUserAggregateKey, MovieRatingUserAggregateValue] = movieRatingVotesStream
            .map[MovieRatingUserAggregateKey, MovieRatingUserAggregateValue](
                new MovieRatingToMovieRatingUserAggregateMapper)
            .groupByKey
            .reduce(new MovieRatingUserAggregateReducer)

        val movieRatingVoteAggregatesTable:
            KTable[MovieRatingAggregateKey, MovieRatingAggregateValue] = movieRatingVoteUserAggregatesTable
            .toStream
            .map[MovieRatingAggregateKey, MovieRatingAggregateValue](
                new MovieRatingUserAggregateToMovieRatingAggregateMapper)
            .groupByKey
            .reduce(new MovieRatingAggregateReducer)

        val movieRatingResultsWithoutTitleStream: KStream[Int, MovieRatingResultWithoutTitle] =
            movieRatingVoteAggregatesTable.toStream
                .map[Int, MovieRatingResultWithoutTitle](
                    new MovieRatingAggregateToMovieRatingResultWithoutTitleMapper)

        val moviesStream: KStream[String, Movie] = builder
            .stream[String, String](MOVIE_TITLES_TOPIC)
            .flatMapValues(value => movieParser.tryParse(value).toIterable)

        val movieTitlesTable: KTable[Int, String] = moviesStream
            .map[Int, String](new MovieToMovieTitleMapper)
            .groupByKey
            .reduce(new NoOpReducer[String])

        movieRatingResultsWithoutTitleStream
            .join[String, MovieRatingResult](movieTitlesTable)(new MovieRatingResultJoiner)
            .mapValues[String](new MovieRatingResultToCsvMapper) // TODO remove
            .to(ETL_RESULT_TOPIC)

        builder.build()
    }
}
