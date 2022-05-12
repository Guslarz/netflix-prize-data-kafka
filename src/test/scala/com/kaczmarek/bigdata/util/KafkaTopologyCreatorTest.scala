package com.kaczmarek.bigdata.util

import com.kaczmarek.bigdata.model._
import com.kaczmarek.bigdata.schema.MessageWithSchema
import com.kaczmarek.bigdata.serde.{JsonDeserializer, JsonWithSchemaDeserializer}
import com.kaczmarek.bigdata.util.KafkaTopologyCreatorTest.DummyTimestampExtractor
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler
import org.apache.kafka.streams.processor.TimestampExtractor
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

import java.io.InputStream
import java.time.Duration
import java.util.Properties
import scala.collection.JavaConverters._
import scala.collection.mutable
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
        pipeInput(testDriver, KafkaTopologyCreator.MOVIE_TITLES_TOPIC, titlesInputStream, None)
        pipeInput(
            testDriver, KafkaTopologyCreator.MOVIE_RATING_VOTES_TOPIC, votesInputStream,
            Some("2000-01-01,-1,0,0") // dummy input to handle suppression
        )

        // then
        try {
            verifyOutput[MovieRatingResultKey, MovieRatingResultValue](
                testDriver, KafkaTopologyCreator.ETL_RESULT_TOPIC, expectedEtlInputStream)
            verifyOutput[AnomalyResultKey, AnomalyResultValue](
                testDriver, KafkaTopologyCreator.ANOMALY_RESULT_TOPIC, expectedAnomalyInputStream)
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
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
            classOf[LogAndContinueExceptionHandler])
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
            classOf[DummyTimestampExtractor])
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

    private def pipeInput(
        testDriver: TopologyTestDriver, topic: String, inputStream: InputStream, dummyInput: Option[String]): Unit = {

        val stringSerializer = new StringSerializer
        val consumerRecordFactory = new ConsumerRecordFactory[String, String](
            topic, stringSerializer, stringSerializer)
        val records = List.concat(
            readLines(inputStream),
            dummyInput
        ).map(line => consumerRecordFactory.create(line))
        testDriver.pipeInput(records.asJava)
    }

    private def verifyOutput[K, V](testDriver: TopologyTestDriver, topic: String, expectedInputStream: InputStream)
                                  (implicit k: ClassTag[K], v: ClassTag[V]): Unit = {
        val keyDeserializer = new JsonWithSchemaDeserializer[K]
        val valueDeserializer = new JsonWithSchemaDeserializer[V]
        val expectedKeyDeserializer = new JsonDeserializer[K]
        val expectedValueDeserializer = new JsonDeserializer[V]
        val expected = readLines(expectedInputStream)
            .map(line => {
                val values = line.split('\t')
                val expectedKey = expectedKeyDeserializer.deserialize("", values(0).getBytes)
                val expectedValue = expectedValueDeserializer.deserialize("", values(1).getBytes)
                (expectedKey, expectedValue)
            })
            .toMap[K, V]
        val result = mutable.Map[K, V]()
        var record: ProducerRecord[MessageWithSchema[K], MessageWithSchema[V]] =
            testDriver.readOutput[MessageWithSchema[K], MessageWithSchema[V]](
                topic, keyDeserializer, valueDeserializer)
        while (record != null) {
            println(s"${record.key().payload} ${record.value().payload}")
            result.put(record.key().payload, record.value().payload)
            record = testDriver.readOutput[MessageWithSchema[K], MessageWithSchema[V]](
                topic, keyDeserializer, valueDeserializer)
        }
        println(s"Topic: $topic")
        println(s"Expected: $expected")
        println(s"Found: $result")
        println()
        assertEquals(expected, result)
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

    private class DummyTimestampExtractor extends TimestampExtractor {

        override def extract(consumerRecord: ConsumerRecord[AnyRef, AnyRef], previousTimestamp: Long): Long = {

            System.currentTimeMillis() + (consumerRecord.value() match {
                case value: MovieRatingVote =>
                    if (value.movieId == -1) {
                        Duration.ofDays(1).toMillis
                    } else {
                        0L
                    }
                case _ => 0L
            })
        }
    }
}
