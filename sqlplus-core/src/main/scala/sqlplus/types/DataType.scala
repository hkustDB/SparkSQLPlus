package sqlplus.types

import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.`type`.SqlTypeName._


abstract class DataType {
    def getScalaTypeName: String
    def fromString(s: String): String

    def castFromAny(v: String): String =
        s"$v.asInstanceOf[$getScalaTypeName]"

    def format(x: String): String = s"$x.toString"
}

case object IntDataType extends DataType {
    override def fromString(s: String): String = s"$s.toInt"

    override def getScalaTypeName: String = "Int"
}

case object LongDataType extends DataType {
    override def fromString(s: String): String = s"$s.toLong"

    override def getScalaTypeName: String = "Long"
}

case object StringDataType extends DataType {
    override def fromString(s: String): String = s

    override def getScalaTypeName: String = "String"
}

case object TimestampDataType extends DataType {
    override def getScalaTypeName: String = "Long"

    override def fromString(s: String): String = {
        s"$s.parseToTimestamp"
    }

    override def format(x: String): String = {
        s"$x.asInstanceOf[Long].printAsString"
    }
}

case object DateDataType extends DataType {
    override def getScalaTypeName: String = "Long"

    override def fromString(s: String): String = throw new UnsupportedOperationException()

    override def format(x: String): String = throw new UnsupportedOperationException()
}

case object DoubleDataType extends DataType {
    override def getScalaTypeName: String = "Double"

    override def fromString(s: String): String = s"$s.toDouble"
}

case object IntervalDataType extends DataType {
    override def getScalaTypeName: String = "Long"

    override def fromString(s: String): String = throw new UnsupportedOperationException("load interval from input is unsupported")
}

object DataType {
    def fromSqlType(sqlTypeName: SqlTypeName): DataType = sqlTypeName match {
        case INTEGER => IntDataType
        case BIGINT => LongDataType
        case VARCHAR => StringDataType
        case TIMESTAMP => TimestampDataType
        case DOUBLE => DoubleDataType
        case DECIMAL => DoubleDataType
        case DATE => DateDataType
        case _ => throw new UnsupportedOperationException(s"SqlType ${sqlTypeName.toString} is unsupported.")
    }

    def fromTypeName(typeName: String): DataType = typeName match {
        case "INTEGER" => IntDataType
        case "BIGINT" => LongDataType
        case "VARCHAR" => StringDataType
        case "TIMESTAMP" => TimestampDataType
        case "DOUBLE" => DoubleDataType
        case "DECIMAL" => DoubleDataType
        case "DATE" => DateDataType
        case _ => throw new UnsupportedOperationException(s"SqlType ${typeName} is unsupported.")
    }

    def isNumericType(dataType: DataType): Boolean = dataType match {
        case IntDataType | LongDataType | DoubleDataType => true
        case _ => false
    }
}
