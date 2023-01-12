package sqlplus.expression

import sqlplus.types.DataType

case class Variable(name: String, dataType: DataType[_]) {
    override def toString: String = {
        s"$name:$dataType"
    }
}
