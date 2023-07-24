package sqlplus.compile

class VariableNameAssigner {
    private var suffix = 1

    def newVariableName(): String = {
        val result = "v" + suffix.toString
        suffix += 1
        result
    }
}
