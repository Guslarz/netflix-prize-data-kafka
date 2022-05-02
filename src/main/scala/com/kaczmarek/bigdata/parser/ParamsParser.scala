package com.kaczmarek.bigdata.parser

import com.kaczmarek.bigdata.model.Params

class ParamsParser extends AbstractParser[Array[String], Params] {

    def parse(args: Array[String]): Params = Params(
        0,
        0,
        0
    )
}
