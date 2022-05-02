package com.kaczmarek.bigdata.util

import com.kaczmarek.bigdata.model.{MovieRatingResult, Params}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{IntegerDeserializer, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.test.{ConsumerRecordFactory, OutputVerifier}
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver}
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
        pipeInput(testDriver, KafkaTopologyCreator.MOVIE_RATING_VOTES_TOPIC, votesInputStream)
        pipeInput(testDriver, KafkaTopologyCreator.MOVIE_TITLES_TOPIC, titlesInputStream)

        // when
        // then
        readLines(expectedInputStream)
            .foreach(line => verifyOutput(testDriver, line))

        testDriver.close()
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
        d = 1,
        l = 1,
        o = 1
    )

    private def readLines(inputStream: InputStream): List[String] = Source
        .fromInputStream(inputStream)
        .getLines()
        .toList

    private def pipeInput(testDriver: TopologyTestDriver, topic: String, inputStream: InputStream): Unit = {
        val stringSerializer = new StringSerializer
        val consumerRecordFactory = new ConsumerRecordFactory[String, String](
            topic, stringSerializer, stringSerializer)
        readLines(inputStream)
            .map(line => consumerRecordFactory.create(line))
            .foreach(testDriver.pipeInput)
    }

    private def verifyOutput(testDriver: TopologyTestDriver, line: String): Unit = {
        val values = line.split(',')
        val expectedKey: Integer = values(0).toInt
        val expectedValue: String = values.tail.mkString(",")
        val outputRecord: ProducerRecord[Integer, String] = testDriver.readOutput(
            KafkaTopologyCreator.ETL_RESULT_TOPIC,
            new IntegerDeserializer,
            new StringDeserializer
        )
        OutputVerifier.compareKeyValue(outputRecord, expectedKey, expectedValue)
    }
}

object KafkaTopologyCreatorTest {

    def getTopologyTestFiles(): java.util.stream.Stream[Arguments] = {
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
