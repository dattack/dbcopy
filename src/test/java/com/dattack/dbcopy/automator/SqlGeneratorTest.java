package com.dattack.dbcopy.automator;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

import static com.dattack.junit.AssertionsExt.assertEquals;

/* package */ final class SqlGeneratorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlGeneratorTest.class);

    private static final String VAR = "var";

    private TableMetadata.TableMetadataBuilder getSimpleBuilder() {

        ObjectName tableName = new ObjectName(null, "MY_SCHEMA", "TEST");
        return new TableMetadata.TableMetadataBuilder(tableName) //
            .withColumn("COLUMN1", 1) //
            .withColumn("COLUMN2", 2) //
            .withColumn("COLUMN3", 3) //
            .withColumn("COLUMN4", 4) //
            .withPrimaryKey("COLUMN1", 1) //
            .withLastAnalyzed(Instant.now()) //
            .withNumRows(10);
    }

    private RangePartition createPartition(int partitionId, int size, boolean lowMode, boolean highMode) {
        return new RangePartition("P" + partitionId, //
                                  String.valueOf(partitionId * 10), lowMode, //
                                  String.valueOf(partitionId * 10 + size), highMode, //
                                  partitionId, partitionId * 1000);
    }

    private TableMetadata.TableMetadataBuilder getSimplePartitionedBuilder(int size, boolean lowMode, boolean highMode)
    {
        return getSimpleBuilder() //
            .withPartition(createPartition(1, size, lowMode, highMode)) //
            .withPartition(createPartition(2, size, lowMode, highMode)) //
            .withPartition(createPartition(3, size, lowMode, highMode)) //
            .withPartitionColumn("COLUMN1", 1);
    }

    private TableMetadata.TableMetadataBuilder getSimplePartitionedBuilder(int size) {
        return getSimplePartitionedBuilder(size, true, true);
    }

    private RangePartition createTwoFieldsPartition(int partitionId, int secondValue, int size, boolean lowMode,
        boolean highMode)
    {
        return new RangePartition("P" + partitionId, //
                                  partitionId * 10 + "," + secondValue, lowMode, //
                                  partitionId * 10 + size + "," + secondValue, highMode, //
                                  partitionId, partitionId * 1000);
    }

    private TableMetadata.TableMetadataBuilder getTwoPartitionFieldsBuilder(int size, boolean lowMode, boolean highMode)
    {
        return getSimpleBuilder() //
            // first range
            .withPartition(createTwoFieldsPartition(1, 1, size, lowMode, highMode)) //
            .withPartition(createTwoFieldsPartition(1, 2, size, lowMode, highMode)) //
            .withPartition(createTwoFieldsPartition(1, 3, size, lowMode, highMode)) //
            // second range
            .withPartition(createTwoFieldsPartition(2, 1, size, lowMode, highMode)) //
            .withPartition(createTwoFieldsPartition(2, 2, size, lowMode, highMode)) //
            .withPartition(createTwoFieldsPartition(2, 3, size, lowMode, highMode)) //
            // third range
            .withPartition(createTwoFieldsPartition(3, 1, size, lowMode, highMode)) //
            .withPartition(createTwoFieldsPartition(3, 2, size, lowMode, highMode)) //
            .withPartition(createTwoFieldsPartition(3, 3, size, lowMode, highMode)) //
            .withPartitionColumn("COLUMN1", 1) //
            .withPartitionColumn("COLUMN2", 2);
    }

    @Test
    void testNoPartitionedTable() {
        TableMetadata table = getSimpleBuilder().build();

        LOGGER.trace("testNoPartitionedTable - Table configuration: {}", table);
        String sql = CodeHelper.generateSelectSqlUsingRangePartitions(table, VAR);
        assertEquals("SELECT * FROM MY_SCHEMA.TEST", sql, "Generated SQL code is not valid");
    }

    @Test
    void testSimplePartitionedTable() {
        TableMetadata table = getSimplePartitionedBuilder(0).build();

        LOGGER.trace("testSimplePartitionedTable - Table configuration: {}", table);
        String sql = CodeHelper.generateSelectSqlUsingRangePartitions(table, VAR);
        assertEquals("SELECT * FROM MY_SCHEMA.TEST WHERE COLUMN1 = ${var.low}", sql,
                     "Generated SQL code is not valid");
    }

    @Test
    void testIncludeExcludePartitionedTable() {
        TableMetadata table = getSimplePartitionedBuilder(1, true, false).build();

        LOGGER.trace("testIncludeExcludePartitionedTable - Table configuration: {}", table);
        String sql = CodeHelper.generateSelectSqlUsingRangePartitions(table, VAR);
        assertEquals("SELECT * FROM MY_SCHEMA.TEST WHERE COLUMN1 >= ${var.low}" //
                         + " AND COLUMN1 < ${var.high}", sql, "Generated SQL code is not valid");
    }

    @Test
    void testExcludeIncludePartitionedTable() {
        TableMetadata table = getSimplePartitionedBuilder(1, false, true).build();

        LOGGER.trace("testIncludeExcludePartitionedTable - Table configuration: {}", table);
        String sql = CodeHelper.generateSelectSqlUsingRangePartitions(table, VAR);
        assertEquals("SELECT * FROM MY_SCHEMA.TEST WHERE COLUMN1 > ${var.low}" //
                         + " AND COLUMN1 <= ${var.high}", sql, "Generated SQL code is not valid");
    }

    @Test
    void testExcludeExcludePartitionedTable() {
        TableMetadata table = getSimplePartitionedBuilder(1, false, false).build();

        LOGGER.trace("testIncludeExcludePartitionedTable - Table configuration: {}", table);
        String sql = CodeHelper.generateSelectSqlUsingRangePartitions(table, VAR);
        assertEquals("SELECT * FROM MY_SCHEMA.TEST WHERE COLUMN1 > ${var.low}" //
                         + " AND COLUMN1 < ${var.high}", sql, "Generated SQL code is not valid");
    }

    @Test
    void testRange10PartitionedTable() {
        TableMetadata table = getSimplePartitionedBuilder(9).build();

        LOGGER.trace("testRange10PartitionedTable - Table configuration: {}", table);
        String sql = CodeHelper.generateSelectSqlUsingRangePartitions(table, VAR);
        assertEquals("SELECT * FROM MY_SCHEMA.TEST WHERE COLUMN1 >= ${var.low}" //
                         + " AND COLUMN1 <= ${var.high}", sql, "Generated SQL code is not valid");
    }

    @Test
    void testTwoPartitionFieldsSize0() {
        TableMetadata table = getTwoPartitionFieldsBuilder(0, true, true).build();

        LOGGER.trace("testRange10PartitionedTable - Table configuration: {}", table);
        String sql = CodeHelper.generateSelectSqlUsingRangePartitions(table, VAR);
        assertEquals("SELECT * FROM MY_SCHEMA.TEST WHERE COLUMN1 = ${var.low[0]}" //
                         + " AND COLUMN2 = ${var.low[1]}", sql, "Generated SQL code is not valid");
    }

    @Test
    void testTwoPartitionFieldsSize1() {
        TableMetadata table = getTwoPartitionFieldsBuilder(1, true, true).build();

        LOGGER.trace("testRange10PartitionedTable - Table configuration: {}", table);
        String sql = CodeHelper.generateSelectSqlUsingRangePartitions(table, VAR);
        assertEquals("SELECT * FROM MY_SCHEMA.TEST" //
                         + " WHERE COLUMN1 >= ${var.low[0]} AND COLUMN1 <= ${var.high[0]}" //
                         + " AND COLUMN2 >= ${var.low[1]} AND COLUMN2 <= ${var.high[1]}", sql,
                     "Generated SQL code is not valid");
    }

    @Test
    void testTwoPartitionFieldsSize1Exclude() {
        TableMetadata table = getTwoPartitionFieldsBuilder(1, false, false).build();

        LOGGER.trace("testTwoPartitionFieldsSize1Exclude - Table configuration: {}", table);
        String sql = CodeHelper.generateSelectSqlUsingRangePartitions(table, VAR);
        assertEquals("SELECT * FROM MY_SCHEMA.TEST" //
                         + " WHERE COLUMN1 > ${var.low[0]} AND COLUMN1 < ${var.high[0]}" //
                         + " AND COLUMN2 > ${var.low[1]} AND COLUMN2 < ${var.high[1]}", sql,
                     "Generated SQL code is not valid");
    }

    @Test
    void testTwoPartitionFieldsMixed() {
        TableMetadata table = getTwoPartitionFieldsBuilder(1, true, true) //
            .withPartition(createTwoFieldsPartition(100, 1, 2, false, false)) //
            .build();

        LOGGER.trace("testTwoPartitionFieldsMixed - Table configuration: {}", table);
        String sql = CodeHelper.generateSelectSqlUsingRangePartitions(table, VAR);
        assertEquals("SELECT * FROM MY_SCHEMA.TEST" //
                         + " WHERE (('${var.low-inclusive}'='INCLUSIVE' AND COLUMN1 >= ${var.low[0]})" //
                         + " OR ('${var.low-inclusive}'='EXCLUSIVE' AND COLUMN1 > ${var.low[0]}))" //
                         + " AND" //
                         + " (('${var.high-inclusive}'='INCLUSIVE' AND COLUMN1 <= ${var.high[0]})" //
                         + " OR ('${var.high-inclusive}'='EXCLUSIVE' AND COLUMN1 < ${var.high[0]}))" //
                         + " AND"  //
                         + " (('${var.low-inclusive}'='INCLUSIVE' AND COLUMN2 >= ${var.low[1]})" //
                         + " OR ('${var.low-inclusive}'='EXCLUSIVE' AND COLUMN2 > ${var.low[1]}))" //
                         + " AND" //
                         + " (('${var.high-inclusive}'='INCLUSIVE' AND COLUMN2 <= ${var.high[1]})" //
                         + " OR ('${var.high-inclusive}'='EXCLUSIVE' AND COLUMN2 < ${var.high[1]}))", sql,
                     "Generated SQL code is not valid");
    }
}