package sqlplus.codegen

case class Parameter(modifier: Option[String], name: String, t: String)

abstract class AbstractScalaCodeGenerator extends CodeGenerator {
    def getPackageName: String

    def getImports: List[String]

    def getType: String

    def getName: String

    def getConstructorParameters: List[Parameter]

    def getExtends: String

    def getSuperClassParameters: List[String]

    def generateBody(builder: StringBuilder): Unit

    override def generate(builder: StringBuilder): Unit = {
        val pkg = getPackageName
        if (pkg != null && pkg != "")
            builder.append("package ").append(pkg).append("\n\n")

        val imports = getImports
        if (imports.nonEmpty) {
            imports.foreach(i => builder.append("import ").append(i).append("\n"))
            builder.append("\n")
        }

        builder.append(getType).append(" ").append(getName)
        val constructorParameters = getConstructorParameters
        if (constructorParameters.nonEmpty) {
            val s = constructorParameters.map(p => {
                if (p.modifier.nonEmpty)
                    p.modifier.get + " " + p.name + ": " + p.t
                else
                    p.name + ": " + p.t
            }).mkString("(", ", ", ")")
            builder.append(s)
        }

        val superClass = getExtends
        if (superClass != null && superClass != "") {
            builder.append(" extends ").append(superClass)
            val superClassParameters = getSuperClassParameters
            if (superClassParameters.nonEmpty)
                builder.append(superClassParameters.mkString("(", ", ", ")"))
        }
        builder.append(" {").append("\n")

        generateBody(builder)

        builder.append("}").append("\n")
    }
}
