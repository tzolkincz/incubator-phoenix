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

import java.io.IOException;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.BinarySerializableComparator;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.FirstLastValueDataContainer;
import org.apache.phoenix.util.FirstLastValueOffsetDataContainer;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.schema.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base client aggregator for (FIRST|LAST|NTH)_VALUE functions
 *
 */
public class FirstLastValueBaseClientAggregator extends BaseAggregator {

	private static final Logger logger = LoggerFactory.getLogger(FirstLastValueBaseClientAggregator.class);
	protected boolean useOffset = false;
	protected int offset = -1;
	protected BinaryComparator topOrder = new BinaryComparator(ByteUtil.EMPTY_BYTE_ARRAY);
	protected byte[] topValue = null;
	protected TreeMap<byte[], byte[]> topValues = new TreeMap<byte[], byte[]>(new BinarySerializableComparator());
	protected boolean isAscending;

	public FirstLastValueBaseClientAggregator() {
		super(SortOrder.getDefault());
	}

	@Override
	public void reset() {
		topOrder = new BinaryComparator(ByteUtil.EMPTY_BYTE_ARRAY);
		topValue = null;
		topValues.clear();
	}

	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		if (useOffset) {
			if (topValues.size() == 0) {
				return false;
			}

			Set<byte[]> keySet;
			if (isAscending) {
				keySet = topValues.keySet();
			} else {
				keySet = topValues.descendingKeySet();
			}

			int counter = offset;
			for (byte[] currentKey : keySet) {
				if (counter-- == 1) {
					ptr.set(topValues.get(currentKey));
					return true;
				}
			}

			//not enought values to return Nth
			return false;
		}

		if (topValue == null) {
			return false;
		}

		ptr.set(topValue);
		return true;
	}

	@Override
	public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {
		if (useOffset) {
			FirstLastValueOffsetDataContainer payload = new FirstLastValueOffsetDataContainer();
			try {
				payload.setPayload(ptr.copyBytes());
				topValues.putAll(payload.getData());
				isAscending = payload.getIsAscending();
			} catch (IOException ex) {
				logger.error(ex.getMessage());
			}
		} else {
			//if is called cause aggregation in ORDER BY clausule
			if (tuple instanceof SingleKeyValueTuple) {
				topValue = ptr.copyBytes();
				return;
			}

			byte[] messageFromRow = new byte[ptr.getSize()];
			System.arraycopy(ptr.get(), ptr.getOffset(), messageFromRow, 0, ptr.getLength());

			FirstLastValueDataContainer payload = new FirstLastValueDataContainer();
			payload.setBytesMessage(messageFromRow);

			byte[] currentValue = payload.getValue();
			byte[] currentOrder = payload.getOrderValue();
			isAscending = payload.getIsAscending();

			boolean isBetter;
			if (isAscending) {
				isBetter = topOrder.compareTo(currentOrder) > 0;
			} else {
				isBetter = topOrder.compareTo(currentOrder) < 0; //desc
			}
			if (topOrder.getValue().length < 1 || isBetter) {
				topOrder = new BinaryComparator(currentOrder);
				topValue = currentValue;
			}
		}
	}

	@Override
	public PDataType getDataType() {
		return PDataType.VARBINARY;
	}

	public void init(int offset) {
		if (offset != 0) {
			useOffset = true;
			this.offset = offset;
		}
	}
}
