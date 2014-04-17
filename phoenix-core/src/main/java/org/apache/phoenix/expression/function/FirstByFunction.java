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
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.FirstByLastByBaseClientAggregator;
import org.apache.phoenix.expression.aggregator.FirstByLastByServerAggregator;
import org.apache.phoenix.parse.FirstByAggregateParseNode;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.PDataType;

@FunctionParseNode.BuiltInFunction(name = FirstByFunction.NAME, nodeClass = FirstByAggregateParseNode.class, args = {
	@FunctionParseNode.Argument(),
	@FunctionParseNode.Argument(),
	@FunctionParseNode.Argument(allowedTypes = {PDataType.INTEGER}, isConstant = true, defaultValue = "0")})
public class FirstByFunction extends FirstByLastByBaseFunction {

	public static final String NAME = "FIRST_BY";

	public FirstByFunction() {
	}

	public FirstByFunction(List<Expression> childExpressions, CountAggregateFunction delegate) {
		super(childExpressions, delegate);
	}

	@Override
	public Aggregator newServerAggregator(Configuration conf) {
		FirstByLastByServerAggregator aggregator = new FirstByLastByServerAggregator();
		aggregator.init(children, true, ((Number) ((LiteralExpression) children.get(2)).getValue()).intValue());

		return aggregator;
	}

	@Override
	public Aggregator newClientAggregator() {
		FirstByLastByBaseClientAggregator aggregator = new FirstByLastByBaseClientAggregator();
		if (children.size() < 3) {
			aggregator.init(children, 0);
		} else {
			aggregator.init(children, ((Number) ((LiteralExpression) children.get(2)).getValue()).intValue());
		}

		return aggregator;
	}
}
