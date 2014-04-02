/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.expression;


import java.sql.SQLException;
import java.util.List;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;

/**
 * Convert string to bytes
 *
 * @author tzolkincz
 */
@FunctionParseNode.BuiltInFunction(name = HexToBytesFunction.NAME, args = {
    @FunctionParseNode.Argument(allowedTypes = {PDataType.VARCHAR})})
public class HexToBytesFunction extends ScalarFunction {

    public static final String NAME = "HEX_TO_BYTES";

    public HexToBytesFunction() {
    }

    public HexToBytesFunction(List<Expression> children) throws SQLException {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression expression = getExpression();
        if (!expression.evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr.getLength() == 0) {
            return true; // expression was evaluated, but evaluated to null
        }

        PDataType type = expression.getDataType();
        String hexStr = (String) type.toObject(ptr);

        byte[] out = new byte[hexStr.length() / 2];
        for (int i = 0; i < hexStr.length(); i = i + 2) {
            try {
                out[i / 2] = (byte) Integer.parseInt(hexStr.substring(i, i + 2), 16);
            } catch (NumberFormatException ex) {
                throw new IllegalDataException("Value " + hexStr.substring(i, i + 2) + " cannot be cast to hex number");
            } catch (StringIndexOutOfBoundsException ex) {
                throw new IllegalDataException("Invalid value length, cannot cast to hex number (" + hexStr + ")");
            }
        }

        ptr.set(out);

        return true;
    }

    @Override
    public PDataType getDataType() {
        return PDataType.VARBINARY;
    }

    @Override
    public boolean isNullable() {
        return getExpression().isNullable();
    }

    private Expression getExpression() {
        return children.get(0);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Integer getMaxLength() {
        return getExpression().getMaxLength();
    }
}
