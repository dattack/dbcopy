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
package com.dattack.dbcopy.engine.functions;

/**
 * Visitor of the hierarchy of AbstractDataFunction.
 *
 * @author cvarela
 * @since 0.3
 */
public interface FunctionVisitor { //NOPMD

    void visit(BigDecimalFunction function) throws Exception;

    void visit(BlobFunction function) throws Exception;

    void visit(BooleanFunction function) throws Exception;

    void visit(ByteFunction function) throws Exception;

    void visit(BytesFunction function) throws Exception;

    void visit(ClobFunction function) throws Exception;

    void visit(DateFunction function) throws Exception;

    void visit(DoubleFunction function) throws Exception;

    void visit(FloatFunction function) throws Exception;

    void visit(IntegerFunction function) throws Exception;

    void visit(LongFunction function) throws Exception;

    void visit(NClobFunction function) throws Exception;

    void visit(NStringFunction function) throws Exception;

    void visit(NullFunction function) throws Exception;

    void visit(ShortFunction function) throws Exception;

    void visit(StringFunction function) throws Exception;

    void visit(TimeFunction function) throws Exception;

    void visit(TimestampFunction function) throws Exception;

    void visit(XmlFunction function) throws Exception;
}
