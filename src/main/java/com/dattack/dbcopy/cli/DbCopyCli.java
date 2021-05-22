/*
 * Copyright (c) 2017, The Dattack team (http://www.dattack.com)
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

import java.util.Arrays;
import java.util.HashSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.dattack.dbcopy.engine.DbCopyEngine;
import com.dattack.jtoolbox.exceptions.DattackParserException;

/**
 * DbCopy CLI tool.
 *
 * @author cvarela
 * @since 0.1
 */
public final class DbCopyCli {

    private static final String FILE_OPTION = "f";
    private static final String LONG_FILE_OPTION = "file";
    private static final String JOB_NAME_OPTION = "j";
    private static final String LONG_JOB_NAME_OPTION = "job";
    private static final String PROPERTIES_OPTION = "p";
    private static final String LONG_PROPERTIES_OPTION = "properties";

    private static Options createOptions() {

        final Options options = new Options();

        options.addOption(Option.builder(FILE_OPTION) //
                .required(true) //
                .longOpt(LONG_FILE_OPTION) //
                .hasArg(true) //
                .argName("DBCOPY_FILE") //
                .desc("the path to the file containing the DBCopy configuration") //
                .build());

        options.addOption(Option.builder(JOB_NAME_OPTION) //
                .required(false) //
                .longOpt(LONG_JOB_NAME_OPTION) //
                .hasArg(true) //
                .argName("JOB_NAME") //
                .desc("the name of the job to execute") //
                .build());

        options.addOption(Option.builder(PROPERTIES_OPTION) //
                .required(false) //
                .longOpt(LONG_PROPERTIES_OPTION) //
                .hasArg(true) //
                .argName("PROPERTIES_FILE") //
                .desc("the path to the file containing execution configuration properties") //
                .build());

        return options;
    }

    /**
     * The <code>main</code> method.
     *
     * @param args
     *            the program arguments
     */
    public static void main(final String[] args) {

        final Options options = createOptions();

        try {
            final CommandLineParser parser = new DefaultParser();
            final CommandLine cmd = parser.parse(options, args);
            final String[] filenames = cmd.getOptionValues(FILE_OPTION);
            final String[] jobNames = cmd.getOptionValues(JOB_NAME_OPTION);
            final String[] propertiesFiles = cmd.getOptionValues(PROPERTIES_OPTION);

            HashSet<String> jobNameSet = null;
            if (jobNames != null) {
                jobNameSet = new HashSet<>(Arrays.asList(jobNames));
            }

            CompositeConfiguration configuration = new CompositeConfiguration();
            if (propertiesFiles != null) {
                for (final String fileName : propertiesFiles) {
                    configuration.addConfiguration(new PropertiesConfiguration(fileName));
                }
            }

            final DbCopyEngine engine = new DbCopyEngine();
            engine.execute(filenames, jobNameSet, configuration);

        } catch (@SuppressWarnings("unused") final ParseException e) {
            showUsage(options);
        } catch (final ConfigurationException | DattackParserException e) {
            System.err.println(e.getMessage());
        }
    }

    private static void showUsage(final Options options) {
        final HelpFormatter formatter = new HelpFormatter();
        final int descPadding = 5;
        final int leftPadding = 4;
        formatter.setDescPadding(descPadding);
        formatter.setLeftPadding(leftPadding);
        final String header = "\n";
        final String footer = "\nPlease report issues at https://github.com/dattack/dbcopy/issues";
        formatter.printHelp("dbcopy ", header, options, footer, true);
    }

    private DbCopyCli() {
        // Main class
    }
}
