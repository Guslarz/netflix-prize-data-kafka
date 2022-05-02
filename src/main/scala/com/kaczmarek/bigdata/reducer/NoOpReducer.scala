package com.kaczmarek.bigdata.reducer

class NoOpReducer[T] extends ((T, T) => T) {

    override def apply(accumulator: T, value: T): T = value
}
