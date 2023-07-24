package sqlplus.types

object DataTypeCasting {
    def promote(xType: DataType, yType: DataType): DataType = {
        (xType, yType) match {
            case (DoubleDataType, LongDataType) => DoubleDataType
            case (DoubleDataType, IntDataType) => DoubleDataType
            case (LongDataType, DoubleDataType) => DoubleDataType
            case (IntDataType, DoubleDataType) => DoubleDataType
            case (IntDataType, LongDataType) => LongDataType
            case (LongDataType, IntDataType) => LongDataType
            case _ =>
                assert(xType == yType)
                xType
        }
    }
}
