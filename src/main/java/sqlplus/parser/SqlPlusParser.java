package sqlplus.parser;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import sqlplus.parser.impl.SqlPlusParserImpl;

public class SqlPlusParser {
    private final static SqlParser.Config CONFIG = SqlParser.config()
            .withParserFactory(SqlPlusParserImpl.FACTORY)
            .withLex(Lex.JAVA);

    public static SqlNodeList parseDdl(String ddl) throws SqlParseException {
        SqlParser parser = SqlParser.create(ddl, CONFIG);
        SqlNodeList nodeList = parser.parseStmtList();
        return nodeList;
    }

    public static SqlNode parseDml(String dml) throws SqlParseException {
        SqlParser parser = SqlParser.create(dml, CONFIG);
        SqlNode node = parser.parseStmt();
        return node;
    }
}
