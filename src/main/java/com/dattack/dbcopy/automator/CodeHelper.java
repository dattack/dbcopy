/*
 * Copyright (c) 2022, The Dattack team (http://www.dattack.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dattack.dbcopy.automator;

import com.dattack.formats.xml.FluentXmlWriter;
import com.dattack.formats.xml.FluentXmlWriterException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility class used to generate SQL code and XML configuration.
 *
 * @author cvarela
 * @since 0.3
 */
public class CodeHelper {

    public static final String EOL = System.lineSeparator();
    public static final String TAB1 = "  ";
    public static final String TAB2 = TAB1 + TAB1;
    public static final String TAB3 = TAB2 + TAB1;
    public static final String NTAB3 = EOL + TAB3;
    public static final String NTAB2 = EOL + TAB2;
    public static final String NTAB1 = EOL + TAB1;

    private static final Logger LOGGER = LoggerFactory.getLogger(CodeHelper.class);

    /**
     * Append an XML representation of the partitioning configuration (partition-range) of a table to the XML writer.
     *
     * @param xml           the XML writer
     * @param tableMetadata the table metadata configuration
     * @param partitionVar  name of the partition variable to use
     */
    public static void appendPartitionRangeXml(FluentXmlWriter xml, TableMetadata tableMetadata, String partitionVar) {
        try {
            xml.writeStartElement("partition-range").writeAttribute("id", partitionVar);
            for (RangePartition rangePartition : tableMetadata.getPartitionList()) {
                xml.writeCharacters(NTAB2).writeEmptyElement("partition") //
                    .writeAttribute("name", rangePartition.getPartitionName()) //
                    .writeAttribute("seq", rangePartition.getPosition()) //
                    .writeAttribute("low-value", rangePartition.getLowValue()) //
                    .writeAttribute("low-inclusive", rangePartition.getLowInclusiveMode().name()) //
                    .writeAttribute("high-value", rangePartition.getHighValue()) //
                    .writeAttribute("high-inclusive", rangePartition.getHighInclusiveMode().name()).writeComment(
                        String.format("num-rows: %,d", rangePartition.getNumRows()));
            }
            xml.writeEndElement(); // partition-range
        } catch (FluentXmlWriterException e) {
            LOGGER.warn(e.getMessage());
        }
    }

    public static void appendPartitionListXml(FluentXmlWriter xml, TableMetadata tableMetadata, String partitionVar) {
        String values = tableMetadata.getPartitionList().stream() //
            .sorted(Comparator.comparing(RangePartition::getPosition)) //
            .map(RangePartition::getPartitionName) //
            .collect(Collectors.joining(","));
        xml.writeStartElement("list") //
            .writeAttribute("id", partitionVar) //
            .writeAttribute("values", values) //
            .writeAttribute("block-size", 1);
    }

    public static String reformat(String text, String spaces) {
        return StringUtils.trimToEmpty(StringUtils.trimToEmpty(text) //
                                           .replaceAll(System.lineSeparator(), System.lineSeparator() + spaces));
    }

    public static String getVariable(String name) {
        return ":" + StringUtils.trim(name);
    }

    /**
     * Generate a SQL-Select statement using filter expressions to access a partition defined as a range of values.
     *
     * @param tableMetadata table configuration
     * @param partitionVar the name of the variable that holds the partition configuration
     * @return the SQL code
     */
    public static String generateSelectSqlUsingRangePartitions(TableMetadata tableMetadata, String partitionVar) {

        List<ColumnMetadata> partitionKeys = tableMetadata.getPartitionKeys().stream().sorted(
            Comparator.comparing(ColumnMetadata::getPartitionSeq)).collect(Collectors.toList());

        StringBuilder sql = new StringBuilder("SELECT * FROM " + tableMetadata.getTableRef());
        if (!partitionKeys.isEmpty()) {
            boolean rangeSize1 = tableMetadata.isAllPartitionsHasSize1();
            RangePartition.InclusiveMode lowInclusiveMode = tableMetadata.getLowInclusiveMode();
            RangePartition.InclusiveMode highInclusiveMode = tableMetadata.getHighInclusiveMode();

            String concatString = " WHERE ";
            for (ColumnMetadata columnMetadata : partitionKeys) {
                int numSeq;
                if (partitionKeys.size() <= 1) {
                    numSeq = -1;
                } else {
                    numSeq = columnMetadata.getPartitionSeq() - 1;
                }
                sql.append(concatString).append(
                    getSqlPartitionColumnFilter(columnMetadata.getName(), numSeq, partitionVar, rangeSize1,
                                                lowInclusiveMode, highInclusiveMode));
                concatString = " AND ";
            }
        }
        return sql.toString();
    }

    private static String getSqlPartitionColumnFilter(String columnName, int seqNum, String partitionVarName,
        boolean rangeSize1, RangePartition.InclusiveMode lowInclusive, RangePartition.InclusiveMode highInclusive)
    {
        String arrayIndex = "";
        if (seqNum >= 0) {
            arrayIndex = "[" + seqNum + "]";
        }

        String result = "";
        if (rangeSize1 && lowInclusive.equals(RangePartition.InclusiveMode.INCLUSIVE) && highInclusive.equals(
            RangePartition.InclusiveMode.INCLUSIVE))
        {
            result = columnName + " = ${" + partitionVarName + ".low" + arrayIndex + "}";
        } else {
            result += getPredicate(lowInclusive, columnName, partitionVarName, arrayIndex, true);
            result += " AND ";
            result += getPredicate(highInclusive, columnName, partitionVarName, arrayIndex, false);
        }
        return result;
    }

    private static String getPredicate(RangePartition.InclusiveMode inclusiveMode, String columnName,
        String partitionVarName, String arrayIndex, boolean low)
    {
        String result;

        String operator = low ? ">" : "<";
        switch (inclusiveMode) {
            case MIXED:
                result = getMixedInclusiveExpr(columnName, partitionVarName, arrayIndex, low);
                break;
            case INCLUSIVE:
                result = String.format("%s %s= ${%s}", columnName, operator, //
                                       getPartitionValueRef(partitionVarName, arrayIndex, low));
                break;
            case EXCLUSIVE:
                result = String.format("%s %s ${%s}", columnName, operator, //
                                       getPartitionValueRef(partitionVarName, arrayIndex, low));
                break;
            default:
                result = "";
        }
        return result;
    }

    private static String getMixedInclusiveExpr(String columnName, String partitionVarName, String arrayIndex,
        boolean low)
    {
        // Example:
        // (
        //     (${partition.low-inclusive} = true AND column >= ${partition.low[0]})
        //     OR
        //     (${partition.low-inclusive} = false AND column > ${partition.low[0]})
        // )

        String partLowInclusiveRef = String.format("%s.%s", partitionVarName, low ? "low-inclusive" : "high-inclusive");
        String partValueRef = getPartitionValueRef(partitionVarName, arrayIndex, low);
        String operator = low ? ">" : "<";

        return String.format("(('${%s}'='INCLUSIVE' AND %s %s= ${%s}) OR ('${%s}'='EXCLUSIVE' AND %s %s ${%s}))", //
                             partLowInclusiveRef, columnName, operator, partValueRef, //
                             partLowInclusiveRef, columnName, operator, partValueRef);
    }

    private static String getPartitionValueRef(String partitionVarName, String arrayIndex, boolean low) {
        return String.format("%s.%s%s", partitionVarName, low ? "low" : "high", arrayIndex);
    }
}
