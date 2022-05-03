package com.kaczmarek.bigdata.operator.filter

class NonNullFilter[K, V] extends ((K, V) => Boolean) {

    override def apply(key: K, value: V): Boolean = value != null
}
