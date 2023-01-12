package sqlplus.expression

import sqlplus.types.DataType

class VariableManager {
    private var suffix: Int = 0

    def getNewVariable(dataType: DataType[_]): Variable = {
        suffix += 1
        Variable(s"v${suffix}", dataType)
    }
}
