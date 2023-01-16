package sqlplus.plan;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.*;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.hint.HintPredicate;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.stream.LogicalDelta;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.util.ImmutableBeans;
import org.apache.calcite.util.Util;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class HackedSqlToRelConverter extends SqlToRelConverter {
    public HackedSqlToRelConverter(
        SqlValidator validator,
        Prepare.CatalogReader catalogReader,
        RelOptCluster cluster
    ) {
        super(
                (rt, qs, sp, vp) -> null,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                getConfig()
        );
    }

    @Override
    protected void convertFrom(Blackboard bb, SqlNode from, List<String> fieldNames) {
        if (from == null) {
            bb.setRoot(LogicalValues.createOneRow(cluster), false);
            return;
        }

        final SqlCall call;
        switch (from.getKind()) {
            case AS:
                call = (SqlCall) from;
                SqlNode firstOperand = call.operand(0);
                final List<String> fieldNameList = new ArrayList<>();
                if (call.operandCount() > 2) {
                    for (SqlNode node : Util.skip(call.getOperandList(), 2)) {
                        fieldNameList.add(((SqlIdentifier) node).getSimple());
                    }
                }
                if (call.operandCount() > 1) {
                    String alias = ((SqlIdentifier)(call.getOperandList().get(1))).getSimple();
                    convertFromWithAlias(bb, firstOperand, fieldNameList, alias);
                } else {
                    super.convertFrom(bb, firstOperand, fieldNameList);
                }
                return;
            default:
                super.convertFrom(bb, from, fieldNames);
        }
    }

    protected void convertFromWithAlias(Blackboard bb, SqlNode from, List<String> fieldNames, String alias) {
        if (from == null) {
            bb.setRoot(LogicalValues.createOneRow(cluster), false);
            return;
        }

        switch (from.getKind()) {
            case SELECT:
            case INTERSECT:
            case EXCEPT:
            case UNION:
                final RelNode rel = convertQueryRecursive(from, false, null).project();
                RelHint hint = RelHint.builder("alias").hintOption("alias", alias).build();
                final List<RelHint> hints = Collections.singletonList(hint);
                RelNode hintedRel = ((Hintable)rel).withHints(hints);
                bb.setRoot(hintedRel, true);
                return;
            case IDENTIFIER:
                convertIdentifierWithAlias(bb, (SqlIdentifier) from, null, null, alias);
                return;
            default:
                convertFrom(bb, from, fieldNames);
        }
    }

    private void convertIdentifierWithAlias(Blackboard bb, SqlIdentifier id, SqlNodeList extendedColumns, SqlNodeList tableHints, String alias) {
        final SqlValidatorNamespace fromNamespace =
                validator.getNamespace(id).resolve();
        if (fromNamespace.getNode() != null) {
            convertFrom(bb, fromNamespace.getNode());
            return;
        }
        final boolean[] usedDataset = {false};
        RelOptTable table =
                SqlValidatorUtil.getRelOptTable(fromNamespace, catalogReader,
                        null, usedDataset);
        if (extendedColumns != null && extendedColumns.size() > 0) {
            assert table != null;
            final SqlValidatorTable validatorTable =
                    table.unwrap(SqlValidatorTable.class);
            final List<RelDataTypeField> extendedFields =
                    SqlValidatorUtil.getExtendedColumns(validator, validatorTable,
                            extendedColumns);
            table = table.extend(extendedFields);
        }
        final RelNode tableRel;
        RelHint hint = RelHint.builder("alias").hintOption("alias", alias).build();
        final List<RelHint> hints = Collections.singletonList(hint);
        tableRel = toRel(table, hints);
        bb.setRoot(tableRel, true);
        if (usedDataset[0]) {
            bb.setDataset(null);
        }
    }

    // create default config with hacked hintStrategyTable
    public final static Config getConfig() {
        // the hacked hintStrategyTable has only one Strategy: "alias", and can not be applied to any rel
        HintStrategyTable hintStrategyTable = HintStrategyTable.builder().hintStrategy("alias", (hint, rel) -> false).build();

        return ImmutableBeans.create(Config.class)
                .withRelBuilderFactory(RelFactories.LOGICAL_BUILDER)
                .withRelBuilderConfigTransform(c -> c.withPushJoinCondition(true))
                .withHintStrategyTable(hintStrategyTable);
    }
}
