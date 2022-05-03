package com.kaczmarek.bigdata.util

import com.kaczmarek.bigdata.model.{MovieRatingResult, Params}
import com.kaczmarek.bigdata.serde.ObjectDeserializer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{IntegerDeserializer, StringSerializer}
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.test.{ConsumerRecordFactory, OutputVerifier}
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

import java.io.InputStream
import java.util.Properties
import scala.collection.JavaConverters._
import scala.io.Source

class KafkaTopologyCreatorTest {

    @ParameterizedTest
    @MethodSource(Array("getTopologyTestFiles"))
    def shouldCreateTopologyThatComputesCorrectResult(
        votesInputStream: InputStream,
        titlesInputStream: InputStream,
        expectedInputStream: InputStream): Unit = {

        // given
        val testDriver = createTestDriver()
        pipeInput(testDriver, KafkaTopologyCreator.MOVIE_TITLES_TOPIC, titlesInputStream)
        pipeInput(testDriver, KafkaTopologyCreator.MOVIE_RATING_VOTES_TOPIC, votesInputStream)

        // when
        // then
        try {
//            testDriver.advanceWallClockTime(1000 * 60 * 60 *10)
            val etlResultStore: KeyValueStore[Int, MovieRatingResult] =
                testDriver.getKeyValueStore(KafkaTopologyCreator.ETL_RESULT_STORE)
            readLines(expectedInputStream)
                .foreach(line => verifyOutput(etlResultStore, line))
        } finally {
            testDriver.close()
        }
    }

    private def createTestDriver(): TopologyTestDriver = {
        val params = createParams()
        val topology = KafkaTopologyCreator.createTopology(params)
        val properties = createProperties(params)
        new TopologyTestDriver(topology, properties)
    }

    private def createProperties(params: Params): Properties = {
        val props = new Properties
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
        props
    }

    private def createParams(): Params = Params(
        anomalyWindowDuration = 30,
        anomalyMinimumVoteCount = 100,
        anomalyMinimumRatingAverage = 4.0
    )

    private def readLines(inputStream: InputStream): List[String] = Source
        .fromInputStream(inputStream)
        .getLines()
        .toList

    private def pipeInput(testDriver: TopologyTestDriver, topic: String, inputStream: InputStream): Unit = {
        val stringSerializer = new StringSerializer
        val consumerRecordFactory = new ConsumerRecordFactory[String, String](
            topic, stringSerializer, stringSerializer)
        val records = readLines(inputStream)
            .map(line => consumerRecordFactory.create(line))
        testDriver.pipeInput(records.asJava)
    }

    private def verifyOutput(store: KeyValueStore[Int, MovieRatingResult], line: String): Unit = {
        val values = line.split(',')
        val expectedKey: Integer = values(0).toInt
        val expectedValue = MovieRatingResult(
            year = values(1).toInt,
            month = values(2).toInt,
            title = values(3),
            voteCount = values(4).toInt,
            ratingSum = values(5).toInt,
            uniqueVoterCount = values(6).toInt
        )
        assertEquals(expectedValue, store.get(expectedKey))
    }
}

object KafkaTopologyCreatorTest {

    def getTopologyTestFiles: java.util.stream.Stream[Arguments] = {
        Stream.iterate(0)(_ + 1)
            .map(i => List(
                s"input-votes${i}.txt",
                s"input-titles${i}.txt",
                s"output${i}.txt"
            ))
            .map(_.map(readResource))
            .takeWhile(_.forall(_ != null))
            .map(files => Arguments.of(files.head, files(1), files(2)))
            .asJava
            .stream()
    }

    private def readResource(name: String): InputStream = getClass
        .getResourceAsStream(name)
}
