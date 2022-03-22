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

import com.dattack.jtoolbox.exceptions.DattackParserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Objects;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Source;
import javax.xml.transform.sax.SAXSource;

/**
 * XML file parser that instantiates the equivalent DbcopyBean object.
 *
 * @author cvarela
 * @since 0.1
 */
@XmlAccessorType(XmlAccessType.FIELD)
public final class DbcopyParser { //NOPMD

    private static final Logger LOGGER = LoggerFactory.getLogger(DbcopyParser.class);

    private DbcopyParser() {
        // static class
    }

    /**
     * Parses an XML file and returns the DbcopyBean that represents the configuration specified in that file.
     *
     * @param file the XML file to parse
     * @return the DbcopyBean containing the configuration
     * @throws DattackParserException if an error occurs when parsing the file
     * @throws NullPointerException when file is null
     */
    public static DbcopyBean parse(final File file) throws DattackParserException {

        if (Objects.isNull(file)) {
            throw new NullPointerException("The 'dbcopy' configuration file can't be null. " //
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

        try (InputStream inputStream = Files.newInputStream(file.toPath())) {
            final InputSource input = new InputSource(inputStream);
            final XMLReader xmlreader = spf.newSAXParser().getXMLReader();
            final Source source = new SAXSource(xmlreader, input);

            final JAXBContext ctx = JAXBContext.newInstance(DbcopyBean.class);
            final Unmarshaller unmarshaller = ctx.createUnmarshaller();
            return (DbcopyBean) unmarshaller.unmarshal(source);
        } catch (JAXBException | IOException | SAXException | ParserConfigurationException e) {
            throw new DattackParserException(e);
        }
    }
}
