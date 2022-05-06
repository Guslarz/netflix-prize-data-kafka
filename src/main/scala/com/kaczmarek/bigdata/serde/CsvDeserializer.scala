package com.kaczmarek.bigdata.serde

import com.kaczmarek.bigdata.util.DateUtils
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer

import java.util.Date
import scala.reflect.ClassTag

class CsvDeserializer[T](implicit private val classTag: ClassTag[T]) extends Deserializer[T] {

    override def deserialize(topic: String, bytes: Array[Byte]): T = {
        try {
            val values = new String(bytes)
                .split(',')
            val constructor = classTag.runtimeClass.getConstructors.head
            val paramTypes = constructor.getParameterTypes
            val params = values
                .zip(paramTypes)
                .map(param => convert(param._1, param._2).asInstanceOf[Object])
            constructor.newInstance(params: _*).asInstanceOf[T]
        } catch {
            case e: Exception => throw new SerializationException("Failed to deserialize", e)
        }
    }

    private def convert(value: String, cls: Class[_]): Any = {
        val CString = classOf[String]
        val CInt = classOf[Int]
        val CDouble = classOf[Double]
        val CDate = classOf[Date]
        cls match {
            case CString => value.asInstanceOf[Any]
            case CInt => value.toInt.asInstanceOf[Any]
            case CDouble => value.toDouble.asInstanceOf[Any]
            case CDate => DateUtils.parseDate(value).asInstanceOf[Any]
            case _ => throw new RuntimeException(s"Unsupported type: $cls")
        }
    }
}
