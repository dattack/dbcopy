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

import com.dattack.dbcopy.beans.DbcopyBean;
import com.dattack.dbcopy.beans.DbcopyJobBean;
import com.dattack.dbcopy.beans.DbcopyParser;
import com.dattack.dbcopy.engine.DbCopyEngine;
import com.dattack.jtoolbox.exceptions.DattackParserException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * DbCopy CLI tool.
 *
 * @author cvarela
 * @since 0.1
 */
public final class DbCopyCli {

    private static final String FILE_OPTION = "f";
    private static final String JOB_NAME_OPTION = "j";
    private static final String LIST_OPTION = "l";
    private static final String LONG_FILE_OPTION = "file";
    private static final String LONG_JOB_NAME_OPTION = "job";
    private static final String LONG_LIST_OPTION = "list";
    private static final String LONG_PROPERTIES_OPTION = "properties";
    private static final String PROPERTIES_OPTION = "p";

    private DbCopyCli() {
        // Main class
    }

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

        options.addOption(Option.builder(LIST_OPTION) //
                              .required(false) //
                              .longOpt(LONG_LIST_OPTION) //
                              .hasArg(false) //
                              .desc("list the name of the jobs contained in the configuration file") //
                              .build());

        return options;
    }

    private static void list(final String[] filenames, final Set<String> taskNames) throws DattackParserException {
        for (final String file : filenames) {
            System.out.format("%n- %s%n", file);
            final DbcopyBean dbcopyBean = DbcopyParser.parse(new File(file));
            for (final DbcopyJobBean dbcopyJobBean : dbcopyBean.getJobList()) {
                if (Objects.isNull(taskNames) || taskNames.contains(dbcopyJobBean.getId())) {
                    System.out.format("    - %s%n", dbcopyJobBean.getId());
                }
            }
        }
    }

    /**
     * The <code>main</code> method.
     *
     * @param args the program arguments
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
            if (Objects.nonNull(jobNames)) {
                jobNameSet = new HashSet<>(Arrays.asList(jobNames));
            }

            if (cmd.hasOption(LIST_OPTION)) {
                list(filenames, jobNameSet);
            } else {

                final AbstractConfiguration configuration = getConfiguration(propertiesFiles);

                final DbCopyEngine engine = new DbCopyEngine();
                engine.execute(filenames, jobNameSet, configuration);
            }

        } catch (@SuppressWarnings("unused") final ParseException e) {
            showUsage(options);
        } catch (final ConfigurationException | DattackParserException e) {
            System.err.println(e.getMessage()); //NOPMD
        }
    }

    private static AbstractConfiguration getConfiguration(
        final String[] propertiesFiles) throws ConfigurationException
    {
        final CompositeConfiguration configuration = new CompositeConfiguration();
        if (Objects.nonNull(propertiesFiles)) {
            for (final String fileName : propertiesFiles) {
                configuration.addConfiguration(new PropertiesConfiguration(fileName)); //NOPMD
            }
        }
        return configuration;
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
}
