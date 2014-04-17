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
package org.apache.phoenix.expression.aggregator;

import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.BinarySerializableComparator;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.FirstByLastByDataContainer;
import org.apache.phoenix.util.FirstByLastByOffsetDataContainer;
import org.apache.phoenix.util.SizedUtil;
import java.io.IOException;
import java.util.List;
import java.util.TreeMap;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author tzolkincz
 */
public class FirstByLastByServerAggregator extends BaseAggregator {

	private static final Logger logger = LoggerFactory.getLogger(FirstByLastByServerAggregator.class);
	protected List<Expression> children;
	protected BinaryComparator topOrder = new BinaryComparator(ByteUtil.EMPTY_BYTE_ARRAY);
	protected byte[] topValue;
	protected boolean useOffset = false;
	protected int offset = -1;
	protected TreeMap<byte[], byte[]> topValues = new TreeMap<byte[], byte[]>(new BinarySerializableComparator());
	protected boolean isAscending;

	public FirstByLastByServerAggregator() {
		super(SortOrder.getDefault());
	}

	@Override
	public void reset() {
		topOrder = new BinaryComparator(ByteUtil.EMPTY_BYTE_ARRAY);
		topValue = null;
		topValues.clear();
		offset = -1;
		useOffset = false;

		super.reset();
	}

	@Override
	public int getSize() {
		return super.getSize() + SizedUtil.IMMUTABLE_BYTES_WRITABLE_SIZE;
	}

	@Override
	public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {

		//set pointer to ordering by field
		children.get(1).evaluate(tuple, ptr);
		byte[] currentOrder = ptr.copyBytes();

		children.get(0).evaluate(tuple, ptr);

		if (useOffset) {
			if (topValues.size() < offset) {
				try {
					topValues.put(currentOrder, ptr.copyBytes());
				} catch (Exception e) {
					logger.error(e.getMessage());
				}
			} else {
				boolean add = false;
				if (isAscending) {
					byte[] lowestKey = topValues.lastKey();
					if (Bytes.compareTo(currentOrder, lowestKey) < 0) {
						topValues.remove(lowestKey);
						add = true;
					}
				} else { //desc
					byte[] highestKey = topValues.firstKey();
					if (Bytes.compareTo(currentOrder, highestKey) > 0) {
						topValues.remove(highestKey);
						add = true;
					}
				}

				if (add) {
					topValues.put(currentOrder, ptr.copyBytes());
				}
			}
		} else {
			boolean isHigher;
			if (isAscending) {
				isHigher = topOrder.compareTo(currentOrder) > 0;
			} else {
				isHigher = topOrder.compareTo(currentOrder) < 0;//desc
			}
			if (topOrder.getValue().length < 1 || isHigher) {

				topValue = ptr.copyBytes();
				topOrder = new BinaryComparator(currentOrder);
			}
		}

	}

	@Override
	public String toString() {
		StringBuilder out = new StringBuilder("FirstAndLastServerAggregator"
				+ " is ascending: " + isAscending + " value=");
		if (useOffset) {
			for (byte[] key : topValues.keySet()) {
				out.append(topValues.get(key));
			}
			out.append(" offset = ").append(offset);
		} else {
			out.append(topValue);
		}

		return out.toString();
	}

	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {

		if (useOffset) {
			if (topValues.size() == 0) {
				return false;
			}

			FirstByLastByOffsetDataContainer payload = new FirstByLastByOffsetDataContainer();
			payload.setIsAscending(isAscending);
			payload.setOffset(offset);
			payload.setData(topValues);

			try {
				ptr.set(payload.getPayload());
			} catch (IOException ex) {
				logger.error(ex.getMessage());
				return false;
			}
			return true;
		}

		if (topValue == null) {
			return false;
		}

		FirstByLastByDataContainer payload = new FirstByLastByDataContainer();
		payload.setOrdertValue(topOrder.getValue());
		payload.setValue(topValue);
		payload.setIsAscending(isAscending);

		ptr.set(payload.getBytesMessage());
		return true;
	}

	@Override
	public PDataType getDataType() {
		return PDataType.VARBINARY;
	}

	public void init(List<Expression> children, boolean isAscending, int offset) {
		this.children = children;
		this.offset = offset;
		if (offset > 0) {
			useOffset = true;
		}

		//set order if modified
		if (children.get(1).getSortOrder() == SortOrder.DESC) {
			this.isAscending = !isAscending;
		} else {
			this.isAscending = isAscending;
		}

	}
}
