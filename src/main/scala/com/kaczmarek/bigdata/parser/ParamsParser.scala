package com.kaczmarek.bigdata.parser

import com.kaczmarek.bigdata.model.Params

class ParamsParser extends AbstractParser[Array[String], Params] {

    def parse(args: Array[String]): Params = Params(
        30,
        100,
        4.0
    )
}
