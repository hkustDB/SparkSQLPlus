package sqlplus.plan.table;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import sqlplus.parser.ddl.SqlCreateTable;
import sqlplus.parser.ddl.SqlTableColumn;
import sqlplus.parser.ddl.SqlTableOption;
import sqlplus.parser.ddl.constraint.SqlTableConstraint;

import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.Collectors;

public class SqlPlusTable extends AbstractTable {
    private String tableName;
    private TableColumn[] tableColumns;
    private String[] tableColumnNames;
    private String[] primaryKeys;
    private HashMap<String, String> tableProperties;

    public SqlPlusTable(SqlCreateTable createTable) {
        assert createTable.getTableName().names.size() == 1;
        this.tableName = createTable.getTableName().names.get(0);

        this.tableColumns = new TableColumn[createTable.getColumnList().size()];
        this.tableColumnNames = new String[createTable.getColumnList().size()];
        for (int i=0; i<createTable.getColumnList().size(); i++) {
            SqlTableColumn sqlTableColumn = (SqlTableColumn) createTable.getColumnList().get(i);
            assert sqlTableColumn.getName().names.size() == 1;
            String columnName = sqlTableColumn.getName().names.get(0);
            String columnType = sqlTableColumn.getType().toString();
            tableColumns[i] = new TableColumn(columnName, columnType);
            tableColumnNames[i] = columnName;
        }

        assert createTable.getTableConstraints().size() <= 1;
        if (createTable.getTableConstraints().size() > 0) {
            SqlTableConstraint constraint = createTable.getTableConstraints().get(0);
            this.primaryKeys = new String[constraint.getColumnNames().length];
            for (int i=0; i<constraint.getColumnNames().length; i++) {
                assert Arrays.stream(tableColumnNames).collect(Collectors.toSet()).contains(constraint.getColumnNames()[i]);
                this.primaryKeys[i] = constraint.getColumnNames()[i];
            }
        } else {
            this.primaryKeys = new String[0];
        }

        this.tableProperties = new HashMap<>();
        for (SqlNode sqlNode : createTable.getPropertyList()) {
            SqlTableOption tableOption = (SqlTableOption) sqlNode;
            tableProperties.put(trim(tableOption.getKey().toString()), trim(tableOption.getValue().toString()));
        }
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
        RelDataTypeFactory.Builder builder = relDataTypeFactory.builder();

        for (TableColumn column : tableColumns) {
            builder.add(column.name, relDataTypeFactory.createSqlType(SqlTypeName.get(column.type)));
        }

        return builder.build();
    }

    public String getTableName() {
        return tableName;
    }

    public TableColumn[] getTableColumns() {
        return tableColumns;
    }

    public String[] getTableColumnNames() {
        return tableColumnNames;
    }

    public String[] getPrimaryKeys() {
        return primaryKeys;
    }

    public HashMap<String, String> getTableProperties() {
        return tableProperties;
    }

    private String trim(String s) {
        assert (s.charAt(0) == '\'' && s.charAt(s.length() - 1) == '\'');
        return s.substring(1, s.length() - 1);
    }

    public static class TableColumn {
        public String name;
        public String type;

        public TableColumn(String name, String type) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }
    }
}
