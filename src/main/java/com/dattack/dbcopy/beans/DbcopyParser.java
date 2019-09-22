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
package com.dattack.dbcopy.beans;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Source;
import javax.xml.transform.sax.SAXSource;

import com.dattack.dbcopy.engine.DataProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import com.dattack.jtoolbox.exceptions.DattackParserException;

/**
 * @author cvarela
 * @since 0.1
 */
public final class DbcopyParser {

    private final static Logger LOGGER = LoggerFactory.getLogger(DbcopyParser.class);

    public static DbcopyBean parse(final File file) throws DattackParserException {

        if (file == null) {
            throw new IllegalArgumentException("The 'dbcopy' configuration file can't be null. " //
                    + "Check your configuration");
        }

        final SAXParserFactory spf = SAXParserFactory.newInstance();
        try {
            spf.setXIncludeAware(true);
        } catch (UnsupportedOperationException e) {
            LOGGER.warn(e.getMessage());
        }
        spf.setNamespaceAware(true);
        spf.setValidating(true);

        try (final FileInputStream fileInputStream = new FileInputStream(file)) {
            final InputSource input = new InputSource(fileInputStream);
            final XMLReader xmlreader = spf.newSAXParser().getXMLReader();
            final Source source = new SAXSource(xmlreader, input);

            final JAXBContext ctx = JAXBContext.newInstance(DbcopyBean.class);
            final Unmarshaller unmarshaller = ctx.createUnmarshaller();
            return (DbcopyBean) unmarshaller.unmarshal(source);
        } catch (JAXBException | IOException | SAXException | ParserConfigurationException e) {
            throw new DattackParserException(e);
        }
    }

    private DbcopyParser() {
        // static class
    }
}
