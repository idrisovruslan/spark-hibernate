package ru.idrisov.datamart;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import ru.idrisov.domain.annotations.Join;
import ru.idrisov.domain.annotations.Joins;
import ru.idrisov.domain.annotations.SourceTableField;
import ru.idrisov.domain.entitys.TableSpark;
import ru.idrisov.domain.enums.WherePlace;

import java.util.*;

import static ru.idrisov.utils.TableUtils.getTableAliasName;
import static ru.idrisov.utils.TableUtils.readTable;

@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@RequiredArgsConstructor
public class DataFrameBuilder {

    private final ApplicationContext applicationContext;
    private final SparkSession sparkSession;
    private final ColumnCreator columnCreator;

    private Dataset<Row> currentDf;
    private Map<String, Dataset<Row>> sourceDfs;
    private TableSpark targetTable;

    public DataFrameBuilder initBuilder(TableSpark targetTable) {
        this.targetTable = targetTable;
        sourceDfs = getSourceDfsMap(targetTable);
        getMainSourceDf();
        return this;
    }

    private Map<String, Dataset<Row>> getSourceDfsMap(TableSpark targetTable) {
        Map<String, Dataset<Row>> sourcesDfs = new HashMap<>();
        getSourceTables(targetTable).forEach(tableSparkClass -> {
            String tableAliasName = getTableAliasName(tableSparkClass);
            Dataset<Row> sourceDf = readTable(sparkSession, applicationContext.getBean(tableSparkClass)).alias(tableAliasName);
            sourcesDfs.put(tableAliasName, sourceDf);
        });
        return sourcesDfs;
    }

    private Set<Class<? extends TableSpark>> getSourceTables(TableSpark targetTable) {
        Set<Class<? extends TableSpark>> set = new HashSet<>();
        Arrays.stream(targetTable.getClass().getDeclaredFields())
                .filter(field -> field.isAnnotationPresent(SourceTableField.class))
                .forEach(field -> set.add(field.getAnnotation(SourceTableField.class).sourceTable()));
        return set;
    }

    public DataFrameBuilder addToDfWhereConditionBeforeJoin() {
        return addToDfWhereCondition(WherePlace.BEFORE_JOIN);
    }

    public DataFrameBuilder addToDfWhereConditionAfterJoin() {
        return addToDfWhereCondition(WherePlace.AFTER_JOIN);
    }

    private DataFrameBuilder addToDfWhereCondition(WherePlace place) {
        List<Column> columnsForWhereBeforeJoin = columnCreator.getColumnsForWhere(targetTable, place);
        Column columnForPreWhere = columnCreator.getColumnFromColumnsList(columnsForWhereBeforeJoin);

        currentDf = currentDf
                .where(
                        columnForPreWhere
                );
        return this;
    }

    public DataFrameBuilder addToDfJoins() {
        for (Join join : targetTable.getClass().getAnnotation(Joins.class).joins()) {

            List<Column> columnsForJoin = columnCreator.getColumnsForJoin(join);
            Column columnForJoin = columnCreator.getColumnFromColumnsList(columnsForJoin);

            currentDf = currentDf
                    .join(sourceDfs.get(getTableAliasName(join.joinedTable())),
                            columnForJoin,
                            join.joinType().getJoinType()
                    );
        }
        return this;
    }

    public Dataset<Row> getResultTargetDf() {
        List<Column> columnsForSelect = columnCreator.getColumnsForSelect(targetTable);
        currentDf = currentDf
                .select(
                        columnsForSelect.toArray(new Column[0])
                );
        return currentDf;
    }

    private void getMainSourceDf() {
        Dataset<Row> sourceDf = sourceDfs.get(sourceDfs.keySet().iterator().next());

        if (sourceDfs.keySet().size() > 1) {
            Join[] joins = targetTable.getClass().getAnnotation(Joins.class).joins();
            sourceDf = sourceDfs.get(getTableAliasName(joins[0].mainTable()));
        }
        currentDf = sourceDf;
    }
}