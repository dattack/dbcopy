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
package com.dattack.dbcopy.cli;

import com.dattack.dbcopy.automator.DatabaseResource;
import com.dattack.dbcopy.automator.JobBuilder;
import com.dattack.dbcopy.automator.ObjectName;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;

public class AutomatorCli {

    private static final String LONG_MAPPING_DIR = "mapping.dir";
    private static final String LONG_OUTPUT_FILENAME = "output";
    private static final String LONG_SOURCE_CATALOG = "source.catalog";
    private static final String LONG_SOURCE_JNDI = "source.jndi";
    private static final String LONG_SOURCE_SCHEMA = "source.schema";
    private static final String LONG_SOURCE_TABLE = "source.table";
    private static final String LONG_TARGET_CATALOG = "target.catalog";
    private static final String LONG_TARGET_JNDI = "target.jndi";
    private static final String LONG_TARGET_SCHEMA = "target.schema";
    private static final String LONG_TARGET_TABLE = "target.table";
    private static final String MAPPING_DIR = "md";
    private static final String MERGE = "merge";
    private static final String OUTPUT_FILENAME = "o";
    private static final String SOURCE_CATALOG = "sc";
    private static final String SOURCE_JNDI = "sj";
    private static final String SOURCE_SCHEMA = "ss";
    private static final String SOURCE_TABLE = "st";
    private static final String TARGET_CATALOG = "tc";
    private static final String TARGET_JNDI = "tj";
    private static final String TARGET_SCHEMA = "ts";
    private static final String TARGET_TABLE = "tt";


    private AutomatorCli() {
        // Main class
    }

    private static Options createOptions() {

        final Options options = new Options();

        // -- source

        options.addOption(Option.builder(SOURCE_CATALOG) //
                              .required(false) //
                              .longOpt(LONG_SOURCE_CATALOG) //
                              .hasArg(true) //
                              .argName("SOURCE_CATALOG") //
                              .desc("name of the source resource catalog") //
                              .build());

        options.addOption(Option.builder(SOURCE_SCHEMA) //
                              .required(false) //
                              .longOpt(LONG_SOURCE_SCHEMA) //
                              .hasArg(true) //
                              .argName("SOURCE_SCHEMA") //
                              .desc("name of the source resource schema") //
                              .build());

        options.addOption(Option.builder(SOURCE_TABLE) //
                              .required(false) //
                              .longOpt(LONG_SOURCE_TABLE) //
                              .hasArg(true) //
                              .argName("SOURCE_TABLE") //
                              .desc("name of the source resource table") //
                              .build());

        options.addOption(Option.builder(SOURCE_JNDI) //
                              .required(true) //
                              .longOpt(LONG_SOURCE_JNDI) //
                              .hasArg(true) //
                              .argName("SOURCE_JNDI") //
                              .desc("name of the JNDI source resource") //
                              .build());

        // -- target

        options.addOption(Option.builder(TARGET_CATALOG) //
                              .required(false) //
                              .longOpt(LONG_TARGET_CATALOG) //
                              .hasArg(true) //
                              .argName("TARGET _CATALOG") //
                              .desc("name of the target resource catalog") //
                              .build());

        options.addOption(Option.builder(TARGET_SCHEMA) //
                              .required(false) //
                              .longOpt(LONG_TARGET_SCHEMA) //
                              .hasArg(true) //
                              .argName("TARGET_SCHEMA") //
                              .desc("name of the target resource schema") //
                              .build());

        options.addOption(Option.builder(TARGET_TABLE) //
                              .required(false) //
                              .longOpt(LONG_TARGET_TABLE) //
                              .hasArg(true) //
                              .argName("TARGET_TABLE") //
                              .desc("name of the target resource table") //
                              .build());

        options.addOption(Option.builder(TARGET_JNDI) //
                              .required(true) //
                              .longOpt(LONG_TARGET_JNDI) //
                              .hasArg(true) //
                              .argName("TARGET_JNDI") //
                              .desc("name of the JNDI target resource") //
                              .build());

        // -- output filename

        options.addOption(Option.builder(OUTPUT_FILENAME) //
                              .required(true) //
                              .longOpt(LONG_OUTPUT_FILENAME) //
                              .hasArg(true) //
                              .argName("OUTPUT_FILENAME") //
                              .desc("path to the output file name") //
                              .build());

        // -- mapping files

        options.addOption(Option.builder(MAPPING_DIR) //
                              .required(false) //
                              .longOpt(LONG_MAPPING_DIR) //
                              .hasArg(true) //
                              .argName("MAPPING_DIRECTORY") //
                              .desc("directory containing the mapping files (currently only " //
                                        + "Oracle GoldenGate files are supported)") //
                              .build());

        // -- merge strategy

        options.addOption(Option.builder(MERGE) //
                              .required(false) //
                              .hasArg(false) //
                              .desc("use MERGE statements instead of INSERT (by default)") //
                              .build());
        return options;
    }

    public static void main(final String[] args) {
        final Options options = createOptions();
        try {
            execute(new DefaultParser().parse(options, args));
        } catch (final ParseException e) {
            System.err.println(e.getMessage()); //NOPMD
            showUsage(options);
        }
    }

    private static void execute(final CommandLine cmd) {

        try {
            DatabaseResource source = new DatabaseResource(new ObjectName(cmd.getOptionValue(SOURCE_CATALOG), //
                                                                          cmd.getOptionValue(SOURCE_SCHEMA), //
                                                                          cmd.getOptionValue(SOURCE_TABLE)), //
                                                           cmd.getOptionValue(SOURCE_JNDI));

            DatabaseResource target = new DatabaseResource(new ObjectName(cmd.getOptionValue(TARGET_CATALOG), //
                                                                          cmd.getOptionValue(TARGET_SCHEMA), //
                                                                          cmd.getOptionValue(TARGET_TABLE)), //
                                                           cmd.getOptionValue(TARGET_JNDI));

            Path mappingDir = getPath(cmd.getOptionValue(MAPPING_DIR));
            Path outputPath = getPath(cmd.getOptionValue(OUTPUT_FILENAME));
            boolean initialLoad = !cmd.hasOption(MERGE);

            new JobBuilder().writeConfiguration(source, target, mappingDir, initialLoad, outputPath);
        } catch (final IOException | SQLException e) {
            System.err.println(e.getMessage()); //NOPMD
        }
    }

    private static Path getPath(String path) {
        Path result = null;
        if (StringUtils.isNotBlank(path)) {
            result = Paths.get(path);
        }
        return result;
    }


    private static void showUsage(final Options options) {
        final HelpFormatter formatter = new HelpFormatter();
        final int descPadding = 3;
        final int leftPadding = 4;
        formatter.setDescPadding(descPadding);
        formatter.setLeftPadding(leftPadding);
        formatter.setWidth(120);
        final String header = "\n";
        final String footer = "\nPlease report issues at https://github.com/dattack/dbcopy/issues";
        formatter.printHelp("automator ", header, options, footer, true);
    }
}
