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
package org.apache.phoenix.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.TreeMap;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Offset implementation for data container for transfer data between server and client aggregation (FirstBy
 * and LastBy functions)
 *
 *
 * @author tzolkincz
 */
public class FirstByLastByOffsetDataContainer extends FirstByLastByDataContainer {

	private static final Logger logger = LoggerFactory.getLogger(FirstByLastByOffsetDataContainer.class);
	protected int offset;
	private TreeMap<byte[], byte[]> data;

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public int getOffset() {
		return offset;
	}

	public void setData(TreeMap<byte[], byte[]> data) {
		this.data = data;
	}

	public byte[] getPayload() throws IOException {

		ByteArrayOutputStream bos = new ByteArrayOutputStream();

		bos.write(useCompression ? (byte) 1 : (byte) 0);
		bos.write(isAscending ? (byte) 1 : (byte) 0);
		bos.write(Bytes.toBytes(offset)); //bos.write(int) writes only last byte

		ObjectOutput out = null;
		try {
			out = new ObjectOutputStream(bos);
			out.writeObject(data);

			byte[] payload = bos.toByteArray();
			return payload;

		} finally {
			if (out != null) {
				out.close();
			}
			bos.close();
		}
	}

	public void setPayload(byte[] payload) throws IOException {

		ByteArrayInputStream bis = new ByteArrayInputStream(payload);

		useCompression = bis.read() != 0;
		isAscending = bis.read() != 0;

		byte[] foo = new byte[4];
		for (int i = 0; i < foo.length; i++) {
			byte[] a = new byte[1];
			bis.read(a);
			foo[i] = a[0];
		}

		offset = fromByteArray(foo);

		ObjectInput in = null;
		try {
			in = new ObjectInputStream(bis);
			data = (TreeMap) in.readObject();
		} catch (ClassNotFoundException ex) {
			logger.error(ex.getMessage());
		} finally {
			if (in != null) {
				in.close();
			}
			bis.close();
		}
	}

	public TreeMap<byte[], byte[]> getData() {
		return data;
	}

	private int fromByteArray(byte[] bytes) {
		return ByteBuffer.wrap(bytes).getInt();
	}
}
