package sqlplus.parser.ddl;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;
import java.util.List;

public class SqlTableOption extends SqlCall {
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("TableOption", SqlKind.OTHER);

    private final SqlNode key;
    private final SqlNode value;

    public SqlTableOption(SqlNode key, SqlNode value, SqlParserPos pos) {
        super(pos);
        this.key = key;
        this.value = value;
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(key, value);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        key.unparse(writer, leftPrec, rightPrec);
        writer.keyword("=");
        value.unparse(writer, leftPrec, rightPrec);
    }

    public SqlNode getKey() {
        return key;
    }

    public SqlNode getValue() {
        return value;
    }
}
