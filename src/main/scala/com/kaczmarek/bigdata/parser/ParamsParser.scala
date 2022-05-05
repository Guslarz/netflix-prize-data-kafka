package com.kaczmarek.bigdata.parser

import com.kaczmarek.bigdata.model.Params

import scala.annotation.tailrec

class ParamsParser extends AbstractParser[Array[String], Params] {

    def parse(args: Array[String]): Params = parseParam(Params(
        server = "",
        anomalyWindowDuration = 30,
        anomalyMinimumVoteCount = 100,
        anomalyMinimumRatingAverage = 4.0
    ), args.toList.tail)

    @tailrec
    private def parseParam(params: Params, args: List[String]): Params = {
        args match {
            case Nil => params
            case "--server" :: value :: tail => parseParam(params.copy(server = value), tail)
            case "-D" :: value :: tail => parseParam(params.copy(anomalyWindowDuration = value.toInt), tail)
            case "-L" :: value :: tail => parseParam(params.copy(anomalyMinimumVoteCount = value.toInt), tail)
            case "-O" :: value :: tail => parseParam(params.copy(anomalyMinimumRatingAverage = value.toDouble), tail)
            case param => throw new RuntimeException(s"Unknown param: $param")
        }
    }
}
