package sqlplus.parser.ddl.constraint;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;

public class SqlTableConstraint extends SqlCall {
    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("SqlTableConstraint", SqlKind.OTHER);

    private final SqlIdentifier constraintName;
    private final SqlLiteral uniqueSpec;
    private final SqlNodeList columns;

    public SqlTableConstraint(
            @Nullable SqlIdentifier constraintName,
            SqlLiteral uniqueSpec,
            SqlNodeList columns,
            SqlParserPos pos) {
        super(pos);
        this.constraintName = constraintName;
        this.uniqueSpec = uniqueSpec;
        this.columns = columns;
    }

    /** Returns whether the constraint is PRIMARY KEY. */
    public boolean isPrimaryKey() {
        return this.uniqueSpec.getValueAs(SqlUniqueSpec.class) == SqlUniqueSpec.PRIMARY_KEY;
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(constraintName, uniqueSpec, columns);
    }

    public Optional<String> getConstraintName() {
        String ret = constraintName != null ? constraintName.getSimple() : null;
        return Optional.ofNullable(ret);
    }

    public SqlNodeList getColumns() {
        return columns;
    }

    /** Returns the columns as a string array. */
    public String[] getColumnNames() {
        return columns.getList().stream()
                .map(col -> ((SqlIdentifier) col).getSimple())
                .toArray(String[]::new);
    }
}
