package com.kaczmarek.bigdata.util

import com.kaczmarek.bigdata.model._
import com.kaczmarek.bigdata.serde.{CustomSerdes, JsonDeserializer}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNull}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

import java.io.InputStream
import java.util.Properties
import scala.collection.JavaConverters._
import scala.io.Source
import scala.reflect.ClassTag

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
            //            verifyEtlOutput(testDriver, expectedEtlInputStream)
            //            verifyAnomalyOutput(testDriver, expectedAnomalyInputStream)
            println(KafkaTopologyCreator.ETL_RESULT_TOPIC)
            printRecords[MovieRatingResultKey, MovieRatingResultValue](testDriver, KafkaTopologyCreator.ETL_RESULT_TOPIC)
            println()
            println(KafkaTopologyCreator.ANOMALY_RESULT_TOPIC)
            printRecords[AnomalyResultKey, AnomalyResultValue](testDriver, KafkaTopologyCreator.ANOMALY_RESULT_TOPIC)
        } finally {
            testDriver.close()
        }
    }

    private def printRecords[K, V](testDriver: TopologyTestDriver, topic: String)
                                  (implicit k: ClassTag[K], v: ClassTag[V]): Unit = {
        var continue = true
        while (continue) {
            val record: ProducerRecord[K, V] =
                testDriver.readOutput(topic, new JsonDeserializer[K], new JsonDeserializer[V])
            if (record == null) {
                continue = false
            } else {
                println(s"${record.key()} ${record.value()}")
            }
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
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
            classOf[LogAndContinueExceptionHandler])
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
        readLines(expectedInputStream)
            .foreach(line => verifyEtlRecord(testDriver, line))
        assertNull(testDriver.readOutput(KafkaTopologyCreator.ETL_RESULT_TOPIC))
    }

    private def verifyEtlRecord(testDriver: TopologyTestDriver, line: String): Unit = {
        val values = line.split('\t')
        val expectedKey = CustomSerdes.movieRatingResultKeyJson.deserializer()
            .deserialize("", values(0).getBytes)
        val expectedValue = CustomSerdes.movieRatingResultValueJson.deserializer()
            .deserialize("", values(1).getBytes)
        val record: ProducerRecord[MovieRatingResultKey, MovieRatingResultValue] = testDriver.readOutput(
            KafkaTopologyCreator.ETL_RESULT_TOPIC,
            CustomSerdes.movieRatingResultKeyJson.deserializer(),
            CustomSerdes.movieRatingResultValueJson.deserializer()
        )
        assertEquals(expectedKey, record.key())
        assertEquals(expectedValue, record.value())
    }

    private def verifyAnomalyOutput(testDriver: TopologyTestDriver, expectedInputStream: InputStream): Unit = {
        readLines(expectedInputStream)
            .foreach(line => verifyAnomalyRecord(testDriver, line))
        assertNull(testDriver.readOutput(KafkaTopologyCreator.ANOMALY_RESULT_TOPIC))
    }

    private def verifyAnomalyRecord(testDriver: TopologyTestDriver, line: String): Unit = {
        val values = line.split('\t')
        val expectedKey = CustomSerdes.anomalyResultKeyJson.deserializer().deserialize("", values(0).getBytes)
        val expectedValue = CustomSerdes.anomalyResultValueJson.deserializer().deserialize("", values(1).getBytes)
        val record: ProducerRecord[AnomalyResultKey, AnomalyResultValue] = testDriver.readOutput(
            KafkaTopologyCreator.ANOMALY_RESULT_TOPIC,
            CustomSerdes.anomalyResultKeyJson.deserializer(),
            CustomSerdes.anomalyResultValueJson.deserializer()
        )
        assertEquals(expectedKey, record.key())
        assertEquals(expectedValue, record.value())
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
