package sqlplus.parser.ddl.constraint;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;

public enum SqlUniqueSpec {
    PRIMARY_KEY("PRIMARY KEY");

    private final String digest;

    SqlUniqueSpec(String digest) {
        this.digest = digest;
    }

    @Override
    public String toString() {
        return this.digest;
    }

    /**
     * Creates a parse-tree node representing an occurrence of this keyword at a particular position
     * in the parsed text.
     */
    public SqlLiteral symbol(SqlParserPos pos) {
        return SqlLiteral.createSymbol(this, pos);
    }
}