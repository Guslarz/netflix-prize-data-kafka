package com.kaczmarek.bigdata.operator.predicate

class TruePredicate[K, V] extends ((K, V) => Boolean) {
    override def apply(key: K, value: V): Boolean = true
}
