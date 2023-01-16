package sqlplus.plan;

import sqlplus.catalog.CatalogManager;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;

public class SqlPlusPlanner {
    private RelOptPlanner relOptPlanner;
    private RelOptCluster relOptCluster;
    private SqlValidator sqlValidator;
    private SqlToRelConverter sqlToRelConverter;

    public SqlPlusPlanner(CatalogManager catalogManager) {
        relOptPlanner = new VolcanoPlanner();
        relOptPlanner.addRelTraitDef(ConventionTraitDef.INSTANCE);

        relOptCluster = RelOptCluster.create(relOptPlanner, new RexBuilder(catalogManager.getTypeFactory()));
        relOptCluster.setMetadataProvider(DefaultRelMetadataProvider.INSTANCE);

        sqlValidator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
                catalogManager.getCatalogReader(), catalogManager.getTypeFactory(),
                SqlValidator.Config.DEFAULT);

        sqlToRelConverter = new HackedSqlToRelConverter(sqlValidator, catalogManager.getCatalogReader(), relOptCluster);
    }

    public RelNode toLogicalPlan(SqlNode sqlNode) {
        SqlNode validatedNode = sqlValidator.validate(sqlNode);
        RelNode logicalPlan = sqlToRelConverter.convertQuery(validatedNode, false, true).rel;
        return optimizeLogicalPlan(logicalPlan);
    }

    private RelNode optimizeLogicalPlan(RelNode logicalPlan) {
        HepProgramBuilder builder = new HepProgramBuilder();

        builder.addRuleInstance(AggregateProjectMergeRule.Config.DEFAULT.toRule());

        HepPlanner hepPlanner = new HepPlanner(builder.build(), logicalPlan.getCluster().getPlanner().getContext());

        hepPlanner.setRoot(logicalPlan);
        RelNode optimizedNode = hepPlanner.findBestExp();

        return optimizedNode;
    }
}
