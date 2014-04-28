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

/**
 * Container for data transfer between server and client aggregation (FIRST|LAST VALUE functions)
 *
 */
public class FirstLastValueDataContainer {

	protected boolean useCompression;
	protected byte[] order;
	protected byte[] value;
	protected boolean isAscending;

	// @TODO commpression
	public void setBytesMessage(byte[] msg) {

		useCompression = false;
		if (msg[0] == (byte) 1) {
			useCompression = true;
		}

		isAscending = false;
		if (msg[1] == (byte) 1) {
			isAscending = true;
		}

		byte lenghtOfSort = msg[2];
		byte lenghtOfValue = msg[3 + lenghtOfSort];

		value = new byte[lenghtOfValue];
		order = new byte[lenghtOfSort];

		System.arraycopy(msg, 3, order, 0, lenghtOfSort);
		System.arraycopy(msg, 4 + lenghtOfSort, value, 0, lenghtOfValue);
	}

	public void setIsAscending(boolean isAscending) {
		this.isAscending = isAscending;
	}

	public boolean getIsAscending() {
		return isAscending;
	}

	public void setCompression(boolean compress) {
		useCompression = compress;
	}

	public boolean getCommpression() {
		return useCompression;
	}

	public void setOrdertValue(byte[] order) {
		this.order = order;
	}

	public byte[] getOrderValue() {
		return order;
	}

	public void setValue(byte[] value) {
		this.value = value;
	}

	public byte[] getValue() {
		return value;
	}

	public byte[] getBytesMessage() {
		//@TODO bool values to bits

		/*
		 MESSAGE STRUCTURE:

		 byte		what
		 0			is commpresed
		 1			order (asc = 0, desc = 1)
		 2			length of sort by value bytes
		 3 to n		bytes sort
		 n + 1		length of value bytes
		 n + 1 to m	bytes value
		 */
		byte sortLength = (byte) order.length;
		byte bytesValueLength = (byte) value.length;

		//meta informations lenght = 4
		byte[] buffer = new byte[sortLength + bytesValueLength + 4];

		if (useCompression) {
			buffer[0] = (byte) 1;
		}
		if (isAscending) {
			buffer[1] = (byte) 1;
		}

		//set second byte as length of sort data
		buffer[2] = sortLength;
		int offset = 3; //set current pointer

		System.arraycopy(order, 0, buffer, offset, order.length);
		offset += order.length;

		//set lenght of data value
		buffer[offset++] = bytesValueLength;

		System.arraycopy(value, 0, buffer, offset, value.length);

		return buffer;
	}
}
