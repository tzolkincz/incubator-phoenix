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

import java.sql.Date;
import java.sql.SQLException;
import java.util.List;
import java.util.TimeZone;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;


/**
 *
 * @author tzolkincz
 */
@FunctionParseNode.BuiltInFunction(name = ConvertTimezoneFunction.NAME, args = {
	@FunctionParseNode.Argument(allowedTypes = {PDataType.DATE}),
	@FunctionParseNode.Argument(allowedTypes = {PDataType.VARCHAR}),
	@FunctionParseNode.Argument(allowedTypes = {PDataType.VARCHAR})})
public class ConvertTimezoneFunction extends ScalarFunction {

	public static final String NAME = "CONVERT_TZ";

	public ConvertTimezoneFunction() {
	}

	public ConvertTimezoneFunction(List<Expression> children) throws SQLException {
		super(children);
	}

	@Override
	public String getName() {
		return NAME;
	}

	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		if (!children.get(0).evaluate(tuple, ptr)) {
			return false;
		}

		Date dateo = (Date) PDataType.DATE.toObject(ptr, children.get(0).getSortOrder());
		Long date = dateo.getTime();

		if (!children.get(1).evaluate(tuple, ptr)) {
			return false;
		}
		String timezoneFrom = Bytes.toString(ptr.get(), ptr.getOffset(), ptr.getLength());

		if (!children.get(2).evaluate(tuple, ptr)) {
			return false;
		}
		String timezoneTo = Bytes.toString(ptr.get(), ptr.getOffset(), ptr.getLength());

		TimeZone tzFrom = TimeZone.getTimeZone(timezoneFrom);
		TimeZone tzTo = TimeZone.getTimeZone(timezoneTo);

		if (!timezoneFrom.equals(tzFrom.getID())) {
			throw new IllegalDataException("Illegal timezone: " + timezoneFrom);
		}
		if (!timezoneTo.equals(tzTo.getID())) {
			throw new IllegalDataException("Illegal timezone: " + timezoneTo);
		}

		long dateInUtc = date - tzFrom.getOffset(date);
		long dateInTo = dateInUtc + tzTo.getOffset(dateInUtc);

		ptr.set(PDataType.DATE.toBytes(new Date(dateInTo)));

		return true;
	}

	@Override
	public PDataType getDataType() {
		return PDataType.DATE;
	}
}
