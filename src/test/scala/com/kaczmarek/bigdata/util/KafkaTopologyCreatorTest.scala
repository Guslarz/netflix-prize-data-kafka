package com.kaczmarek.bigdata.util

import com.kaczmarek.bigdata.model.{AnomalyResultKey, AnomalyResultValue, MovieRatingResult, Params}
import com.kaczmarek.bigdata.serde.CustomSerdes
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.test.ConsumerRecordFactory
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
        expectedEtlInputStream: InputStream,
        expectedAnomalyInputStream: InputStream): Unit = {

        // given
        val testDriver = createTestDriver()

        // when
        pipeInput(testDriver, KafkaTopologyCreator.MOVIE_TITLES_TOPIC, titlesInputStream)
        pipeInput(testDriver, KafkaTopologyCreator.MOVIE_RATING_VOTES_TOPIC, votesInputStream)

        // then
        try {
            verifyEtlOutput(testDriver, expectedEtlInputStream)
            verifyAnomalyOutput(testDriver, expectedAnomalyInputStream)
        } finally {
            testDriver.close()
        }
    }

    private def createTestDriver(): TopologyTestDriver = {
        val params = createParams()
        val topology = KafkaTopologyCreator.createTopology(params)
        val properties = createProperties()
        new TopologyTestDriver(topology, properties)
    }

    private def createProperties(): Properties = {
        val props = new Properties
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
        props
    }

    private def createParams(): Params = Params(
        server = "",
        anomalyWindowDuration = 1,
        anomalyMinimumVoteCount = 3,
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

    private def verifyEtlOutput(testDriver: TopologyTestDriver, expectedInputStream: InputStream): Unit = {
        val etlResultStore: KeyValueStore[Int, MovieRatingResult] =
            testDriver.getKeyValueStore(KafkaTopologyCreator.ETL_RESULT_STORE)
        readLines(expectedInputStream)
            .foreach(line => verifyEtlRecord(etlResultStore, line))
    }

    private def verifyEtlRecord(store: KeyValueStore[Int, MovieRatingResult], line: String): Unit = {
        val values = line.split('\t')
        val expectedKey: Integer = values(0).toInt
        val expectedValue = CustomSerdes.movieRatingResultJson.deserializer()
            .deserialize("", values(1).getBytes)
        assertEquals(expectedValue, store.get(expectedKey))
    }

    private def verifyAnomalyOutput(testDriver: TopologyTestDriver, expectedInputStream: InputStream): Unit = {
        val anomalyResultStore: KeyValueStore[AnomalyResultKey, AnomalyResultValue] =
            testDriver.getKeyValueStore(KafkaTopologyCreator.ANOMALY_RESULT_STORE)
        readLines(expectedInputStream)
            .foreach(line => verifyAnomalyRecord(anomalyResultStore, line))
    }

    private def verifyAnomalyRecord(store: KeyValueStore[AnomalyResultKey, AnomalyResultValue], line: String): Unit = {
        val values = line.split('\t')
        val expectedKey = CustomSerdes.anomalyResultKeyJson.deserializer().deserialize("", values(0).getBytes)
        val expectedValue = CustomSerdes.anomalyResultValueJson.deserializer().deserialize("", values(1).getBytes)
        assertEquals(expectedValue, store.get(expectedKey))
    }
}

object KafkaTopologyCreatorTest {

    def getTopologyTestFiles: java.util.stream.Stream[Arguments] =
        Stream.iterate(0)(_ + 1)
            .map(i => List(
                s"input-votes$i.txt",
                s"input-titles$i.txt",
                s"output-etl$i.txt",
                s"output-anomaly$i.txt"
            ))
            .map(_.map(readResource))
            .takeWhile(_.forall(_ != null))
            .map(files => Arguments.of(files.head, files(1), files(2), files(3)))
            .asJava
            .stream()

    private def readResource(name: String): InputStream = getClass
        .getResourceAsStream(name)
}
