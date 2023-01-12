package sqlplus.types

import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.`type`.SqlTypeName.{BIGINT, INTEGER, VARCHAR}

import scala.reflect.runtime.universe.{TypeTag, typeOf}

abstract class DataType[T: TypeTag] {
    def fromString(s: String): String

    final def castFromAny(s: String): String = s"$s.asInstanceOf[${typeOf[T]}]"

    override def toString: String = typeOf[T].toString
}

case object IntDataType extends DataType[Int] {
    override def fromString(s: String): String = s"$s.toInt"
}

case object LongDataType extends DataType[Long] {
    override def fromString(s: String): String = s"$s.toLong"
}

case object StringDataType extends DataType[String] {
    override def fromString(s: String): String = s
}

object DataType {
    def fromSqlType(sqlTypeName: SqlTypeName): DataType[_] = sqlTypeName match {
        case INTEGER => IntDataType
        case BIGINT => IntDataType  // TODO: fix the compare in helper class, switch back to LongDataType
        case VARCHAR => StringDataType
        case _ => throw new UnsupportedOperationException(s"SqlType ${sqlTypeName.toString} is unsupported.")
    }

    def isNumericType(dataType: DataType[_]): Boolean = dataType match {
        case IntDataType | LongDataType => true
        case _ => false
    }
}
