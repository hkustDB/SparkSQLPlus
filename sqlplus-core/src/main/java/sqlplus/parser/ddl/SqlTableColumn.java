package sqlplus.parser.ddl;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;
import java.util.List;

public class SqlTableColumn extends SqlCall {
    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("COLUMN_DECL", SqlKind.COLUMN_DECL);

    private final SqlIdentifier name;

    private SqlDataTypeSpec type;

    public SqlTableColumn(SqlParserPos pos, SqlIdentifier name, SqlDataTypeSpec type) {
        super(pos);
        this.name = name;
        this.type = type;
    }

    public SqlIdentifier getName() {
        return name;
    }

    public SqlDataTypeSpec getType() {
        return type;
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, type);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        name.unparse(writer, leftPrec, rightPrec);
        type.unparse(writer, leftPrec, rightPrec);
    }
}
