package sqlplus.catalog;

import sqlplus.parser.ddl.SqlCreateTable;
import sqlplus.plan.table.SqlPlusTable;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import java.util.Arrays;
import java.util.Collections;

public class CatalogManager {
    private CalciteSchema schema;
    private RelDataTypeFactory typeFactory;
    private CalciteConnectionConfig connectionConfig;
    private CalciteCatalogReader catalogReader;

    public CatalogManager() {
        schema = CalciteSchema.createRootSchema(true);
        typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        connectionConfig = CalciteConnectionConfig.DEFAULT.set(CalciteConnectionProperty.CASE_SENSITIVE, "false");
        catalogReader = new CalciteCatalogReader(schema, Collections.emptyList(), typeFactory, connectionConfig);
    }

    public void register(SqlNodeList nodeList) {
        for (SqlNode node : nodeList) {
            SqlCreateTable createTable = (SqlCreateTable) node;
            SqlPlusTable table = new SqlPlusTable(createTable);
            register(table.getTableName(), table);
        }
    }

    public void register(String tableName, Table table) {
        schema.add(tableName, table);
    }

    public CalciteSchema getSchema() {
        return schema;
    }

    public RelDataTypeFactory getTypeFactory() {
        return typeFactory;
    }

    public CalciteCatalogReader getCatalogReader() {
        return catalogReader;
    }
}
