package sqlplus.codegen

trait CodeGenerator {
    def generate(builder: StringBuilder): Unit

    def indent(builder: StringBuilder, n: Int): StringBuilder = {
        assert(n >= 0)
        for (_ <- 0 until n)
            builder.append("\t")
        builder
    }

    def newLine(builder: StringBuilder, n: Int = 1): StringBuilder = {
        assert(n >= 0)
        for (_ <- 0 until n)
            builder.append("\n")
        builder
    }
}
