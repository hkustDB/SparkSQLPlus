package sqlplus.parser.ddl;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import sqlplus.parser.ddl.constraint.SqlTableConstraint;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

public class SqlCreateTable extends SqlCreate {
    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE);

    private final SqlIdentifier tableName;

    private final SqlNodeList columnList;

    private final List<SqlTableConstraint> tableConstraints;

    private final SqlNodeList propertyList;

    private final boolean ifNotExists;

    public SqlCreateTable(SqlParserPos pos,
                          SqlIdentifier tableName,
                          SqlNodeList columnList,
                          List<SqlTableConstraint> tableConstraints,
                          SqlNodeList propertyList,
                          boolean ifNotExists) {
        super(OPERATOR, pos, false, ifNotExists);
        this.tableName = tableName;
        this.columnList = columnList;
        this.tableConstraints = tableConstraints;
        this.propertyList = propertyList;
        this.ifNotExists = ifNotExists;
    }

    public SqlIdentifier getTableName() {
        return tableName;
    }

    public SqlNodeList getColumnList() {
        return columnList;
    }

    public List<SqlTableConstraint> getTableConstraints() {
        return tableConstraints;
    }

    public SqlNodeList getPropertyList() {
        return propertyList;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(
                tableName,
                columnList,
                propertyList);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        writer.keyword("TABLE");
        if (isIfNotExists()) {
            writer.keyword("IF NOT EXISTS");
        }
        tableName.unparse(writer, leftPrec, rightPrec);
        if (columnList.size() > 0) {
            SqlWriter.Frame frame =
                    writer.startList(SqlWriter.FrameTypeEnum.create("sds"), "(", ")");
            for (SqlNode column : columnList) {
                printIndent(writer);
                column.unparse(writer, leftPrec, rightPrec);
            }

            writer.newlineAndIndent();
            writer.endList(frame);
        }

        if (this.propertyList.size() > 0) {
            writer.keyword("WITH");
            SqlWriter.Frame withFrame = writer.startList("(", ")");
            for (SqlNode property : propertyList) {
                printIndent(writer);
                property.unparse(writer, leftPrec, rightPrec);
            }
            writer.newlineAndIndent();
            writer.endList(withFrame);
        }
    }

    private void printIndent(SqlWriter writer) {
        writer.sep(",", false);
        writer.newlineAndIndent();
        writer.print("  ");
    }

    public static class TableCreationContext {
        public List<SqlNode> columnList = new ArrayList<>();
        public List<SqlTableConstraint> constraints = new ArrayList<>();
    }
}
