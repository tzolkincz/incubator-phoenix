/*
 * Copyright 2014 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.expression.function;

import java.util.List;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.schema.tuple.Tuple;

/**
 *
 * @author tzolkincz
 */
abstract public class FirstLastBaseFunction extends DelegateConstantToCountAggregateFunction {

	public static String NAME = null;

	public FirstLastBaseFunction() {
	}

	public FirstLastBaseFunction(List<Expression> childExpressions, CountAggregateFunction delegate) {
		super(childExpressions, delegate);
	}

	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		boolean wasEvaluated = super.evaluate(tuple, ptr);
		if (!wasEvaluated) {
			return false;
		}
		if (isConstantExpression()) {
			getAggregatorExpression().evaluate(tuple, ptr);
		}
		return true;
	}

	@Override
	public String getName() {
		return NAME;
	}
}
