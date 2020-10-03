/*
 * Copyright (c) 2020, The Dattack team (http://www.dattack.com)
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
package com.dattack.dbcopy.engine.export;

import com.dattack.dbcopy.beans.ExportOperationBean;
import com.dattack.dbcopy.engine.export.csv.CsvExportOperationFactory;
import com.dattack.dbcopy.engine.export.parquet.ParquetExportOperationFactory;
import org.apache.commons.configuration.AbstractConfiguration;

/**
 * @author cvarela
 * @since 0.1
 */
public class ExportOperationFactoryProducer {

    private ExportOperationFactoryProducer() {
        // static class
    }

    public static ExportOperationFactory getFactory(final ExportOperationBean bean,
                                                    final AbstractConfiguration configuration) {

        ExportOperationFactory factory = null;
        switch (bean.getType()) {
            case PARQUET:
                factory = new ParquetExportOperationFactory(bean, configuration);
                break;
            case CSV:
            default:
                factory = new CsvExportOperationFactory(bean, configuration);
        }
        return factory;
    }
}
