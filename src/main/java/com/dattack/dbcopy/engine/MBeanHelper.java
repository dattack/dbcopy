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
package com.dattack.dbcopy.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

/**
 * Helper class for MBean management.
 *
 * @author cvarela
 * @since 0.1
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public final class MBeanHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(MBeanHelper.class);

    private MBeanHelper() {
        // static class
    }

    public static ObjectName createObjectName(final String type, final String name) {
        ObjectName objectName = null;
        try {
            objectName = new ObjectName("com.dattack.dbcopy:type=" + type + ",name=" + name);
        } catch (MalformedObjectNameException e) {
            LOGGER.warn(e.getMessage(), e);
        }
        return objectName;
    }

    public static ObjectInstance registerMBean(final String type, final String name, final Object object) {
        return registerMBean(createObjectName(type, name), object);
    }

    public static ObjectInstance registerMBean(final ObjectName name, final Object object) {

        if (name == null || object == null) {
            LOGGER.warn("Unable to register MBean (name: {}, class: {})", name, //
                    object == null ? "null" : object.getClass());
            return null;
        }

        LOGGER.info("Registering MBean (name: {}, class: {})", name, object.getClass());
        ObjectInstance objectInstance = null;
        try {
            final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            objectInstance = mbs.registerMBean(object, name);
        } catch (InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException e) {
            LOGGER.warn(e.getMessage(), e);
        }
        return objectInstance;
    }

    public static void unregisterMBean(final ObjectName name) {
        try {
            final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.unregisterMBean(name);
        } catch (MBeanRegistrationException | InstanceNotFoundException e) {
            LOGGER.warn(e.getMessage(), e);
        }
    }
}
