/*
 * Copyright (c) 2021, The Dattack team (http://www.dattack.com)
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
package com.dattack.dbcopy.engine.datatype;

/**
 * Visitor of the internal data type hierarchy.
 *
 * @author cvarela
 * @since 0.3
 */
public interface DataTypeVisitor {

    void visit(BigDecimalType type) throws Exception;

    void visit(BlobType type) throws Exception;

    void visit(BooleanType type) throws Exception;

    void visit(ByteType type) throws Exception;

    void visit(BytesType type) throws Exception;

    void visit(ClobType type) throws Exception;

    void visit(DateType type) throws Exception;

    void visit(DoubleType type) throws Exception;

    void visit(FloatType type) throws Exception;

    void visit(IntegerType type) throws Exception;

    void visit(LongType type) throws Exception;

    void visit(NClobType type) throws Exception;

    void visit(NStringType type) throws Exception;

    void visit(NullType type) throws Exception;

    void visit(ShortType type) throws Exception;

    void visit(StringType type) throws Exception;

    void visit(TimeType type) throws Exception;

    void visit(TimestampType type) throws Exception;

    void visit(XmlType type) throws Exception;
}
