package com.kaczmarek.bigdata.parser

import org.slf4j.LoggerFactory

abstract class AbstractParser[IN, OUT] {

    private val logger = LoggerFactory.getLogger(getClass)

    def tryParse(input: IN): Option[OUT] = {
        try {
            Some(parse(input))
        } catch {
            case e: Throwable =>
                logger.error("Failed to parse: {}", input, e)
                None
        }
    }

    def parse(input: IN): OUT
}
