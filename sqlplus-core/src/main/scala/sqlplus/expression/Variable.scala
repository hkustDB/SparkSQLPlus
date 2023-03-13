package sqlplus.expression

import sqlplus.types.DataType

case class Variable(name: String, dataType: DataType) {
    override def toString: String = {
        s"$name:$dataType"
    }
}
