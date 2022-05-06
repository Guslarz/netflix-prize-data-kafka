package com.kaczmarek.bigdata.serde

import com.kaczmarek.bigdata.model.MovieRatingJoinedResult
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{BeforeEach, Test}

class CsvSerdeTest {

    private var uut: CsvSerde[MovieRatingJoinedResult] = _

    @BeforeEach
    def setUp(): Unit = {
        uut = new CsvSerde
    }

    @Test
    def shouldConvertBothWaysWithObject(): Unit = {
        // given
        val expected = MovieRatingJoinedResult(
            year = 1,
            month = 2,
            title = "title",
            voteCount = 3,
            ratingSum = 4,
            uniqueVoterCount = 5
        )

        // when
        val serialized = uut.serializer().serialize("", expected)
        val result = uut.deserializer().deserialize("", serialized)

        // then
        assertEquals(expected, result)
    }

    @Test
    def shouldConvertBothWaysWithString(): Unit = {
        // given
        val expected =
            "1,2,title,3,4,5"

        // when
        val deserialized = uut.deserializer().deserialize("", expected.getBytes)
        val result = new String(uut.serializer().serialize("", deserialized))

        // then
        assertEquals(expected, result)
    }
}
