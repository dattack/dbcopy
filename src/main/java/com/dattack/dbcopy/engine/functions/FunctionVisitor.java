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

    void visit(BigDecimalFunction function) throws FunctionException;

    void visit(BlobFunction function) throws FunctionException;

    void visit(BooleanFunction function) throws FunctionException;

    void visit(ByteFunction function) throws FunctionException;

    void visit(BytesFunction function) throws FunctionException;

    void visit(ClobFunction function) throws FunctionException;

    void visit(DateFunction function) throws FunctionException;

    void visit(DoubleFunction function) throws FunctionException;

    void visit(FloatFunction function) throws FunctionException;

    void visit(IntegerFunction function) throws FunctionException;

    void visit(LongFunction function) throws FunctionException;

    void visit(NClobFunction function) throws FunctionException;

    void visit(NStringFunction function) throws FunctionException;

    void visit(NullFunction function) throws FunctionException;

    void visit(ShortFunction function) throws FunctionException;

    void visit(StringFunction function) throws FunctionException;

    void visit(TimeFunction function) throws FunctionException;

    void visit(TimestampFunction function) throws FunctionException;

    void visit(XmlFunction function) throws FunctionException;
}
