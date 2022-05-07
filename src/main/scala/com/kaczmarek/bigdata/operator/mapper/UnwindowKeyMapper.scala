package com.kaczmarek.bigdata.operator.mapper

import org.apache.kafka.streams.kstream.Windowed

class UnwindowKeyMapper[K, V] extends ((Windowed[K], V) => (K, V)) {

    override def apply(key: Windowed[K], value: V): (K, V) = (
        key.key(),
        value
    )
}
