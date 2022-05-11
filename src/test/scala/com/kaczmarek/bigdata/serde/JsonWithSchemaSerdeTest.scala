package com.kaczmarek.bigdata.serde

import com.kaczmarek.bigdata.model.{AnomalyResultKey, AnomalyResultValue, MovieRatingResultKey, MovieRatingResultValue}
import com.kaczmarek.bigdata.schema.{MessageWithSchema, SchemaKeyValueWrapper}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.connect.json.JsonConverter
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}
import org.skyscreamer.jsonassert.JSONAssert

import scala.collection.JavaConverters.mapAsJavaMapConverter

class JsonWithSchemaSerdeTest {

    @ParameterizedTest
    @MethodSource(Array("arguments"))
    def shouldProperlySerializeObjectsWithSchemas[K, V](
        keySchemaFile: String,
        valueSchemaFile: String,
        keySerde: Serde[MessageWithSchema[K]],
        valueSerde: Serde[MessageWithSchema[V]],
        key: K,
        value: V
    ): Unit = {
        // given
        val wrapper = new SchemaKeyValueWrapper[K, V](
            keySchemaFile,
            valueSchemaFile
        )
        val keyConverter = new JsonConverter
        val valueConverter = new JsonConverter
        val config = Map[String, Any](
            ("schemas.enable", true)
        ).asJava
        keyConverter.configure(config, true)
        valueConverter.configure(config, false)

        // when
        val wrappedPair = wrapper.apply(key, value)
        val serializedKey = keySerde.serializer().serialize("", wrappedPair._1)
        val serializedValue = valueSerde.serializer().serialize("", wrappedPair._2)

        val deserializedKey = keyConverter.toConnectData("", serializedKey)
        val deserializedValue = valueConverter.toConnectData("" , serializedValue)

        val serializedKey2 = keyConverter
            .fromConnectData("", deserializedKey.schema(), deserializedKey.value())
        val serializedValue2 = keyConverter
            .fromConnectData("", deserializedValue.schema(), deserializedValue.value())

        // then
        JSONAssert.assertEquals(new String(serializedKey), new String(serializedKey2), false)
        JSONAssert.assertEquals(new String(serializedValue), new String(serializedValue2), false)
    }
}

object JsonWithSchemaSerdeTest {

    def arguments(): java.util.stream.Stream[Arguments] =
        java.util.stream.Stream.of(
            Arguments.of(
                "movie-rating-result-key.json",
                "movie-rating-result-value.json",
                new JsonWithSchemaSerde[MovieRatingResultKey](),
                new JsonWithSchemaSerde[MovieRatingResultValue](),
                MovieRatingResultKey(
                    movieId = 1,
                    yearMonth = "2022-05"
                ),
                MovieRatingResultValue(
                    title = "ASDF",
                    voteCount = 2,
                    ratingSum = 3,
                    uniqueVoterCount = 4
                )
            ),
            Arguments.of(
                "anomaly-result-key.json",
                "anomaly-result-value.json",
                new JsonWithSchemaSerde[AnomalyResultKey](),
                new JsonWithSchemaSerde[AnomalyResultValue](),
                AnomalyResultKey(
                    movieId = 1,
                    windowStart = "2022-05-11",
                    windowEnd = "2022-05-12"
                ),
                AnomalyResultValue(
                    title = "ASDF",
                    voteCount = 2,
                    ratingAverage = 3
                )
            )
        )
}
