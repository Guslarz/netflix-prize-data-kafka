package com.kaczmarek.bigdata.operator.reducer

import org.slf4j.LoggerFactory

class NoOpReducer[T] extends ((T, T) => T) {

    private val logger = LoggerFactory.getLogger(getClass)

    override def apply(accumulator: T, value: T): T = {
        logger.info(">> {} {}", accumulator, value: Any)
        logger.info("<< {}", value)
        value
    }
}
