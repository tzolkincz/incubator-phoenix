/*
 * Copyright 2014 The Apache Software Foundation
 *
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
package org.apache.phoenix.end2end;

import static org.junit.Assert.*;
import java.sql.*;
import org.junit.Test;

/**
 *
 * @author tzolkincz
 */
public class FirstByLastByFunctionIT extends BaseHBaseManagedTimeIT {

	private void prepareTable() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		String ddl = "CREATE TABLE IF NOT EXISTS firstBy "
				+ "(id INTEGER NOT NULL PRIMARY KEY, page_id INTEGER,"
				+ " date INTEGER, \"value\" INTEGER)";
		conn.createStatement().execute(ddl);

		//in value column will be 0, 2, 4, 6, 8, so 8 is the biggest
		for (int i = 0; i < 5; i++) {
			conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\")"
					+ " VALUES (" + i + ", 8, " + 100 * i + ", " + i * 2 + ")");
		}

		conn.commit();
	}

	@Test
	public void testFirst() throws Exception {
		prepareTable();
		Connection conn = DriverManager.getConnection(getUrl());

		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT FIRST_BY(\"value\", date) FROM firstBy GROUP BY page_id");

		assertTrue(rs.next());
		assertEquals(rs.getInt(1), 0);
		assertFalse(rs.next());
	}

	@Test
	public void offsetValue() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS firstBy "
				+ "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
				+ " date INTEGER, \"value\" UNSIGNED_LONG)";
		conn.createStatement().execute(ddl);

		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (1, 8, 0, 300)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (2, 8, 1, 7)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (3, 8, 2, 9)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (4, 8, 3, 4)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (5, 8, 4, 2)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (6, 8, 5, 150)");
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery("SELECT FIRST_BY(\"value\", date, 2) FROM firstBy GROUP BY page_id");

		assertTrue(rs.next());
		assertEquals(rs.getInt(1), 7);
		assertFalse(rs.next());
	}

	@Test
	public void offsetValueLast() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS firstBy "
				+ "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
				+ " date INTEGER, \"value\" UNSIGNED_LONG)";
		conn.createStatement().execute(ddl);

		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (1, 8, 0, 300)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (2, 8, 1, 7)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (3, 8, 2, 9)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (4, 8, 3, 4)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (5, 8, 4, 2)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (6, 8, 5, 150)");
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT LAST_BY(\"value\", date, 2) FROM firstBy GROUP BY page_id");

		assertTrue(rs.next());
		assertEquals(rs.getInt(1), 2);
		assertFalse(rs.next());
	}

	@Test
	public void offsetValueLastMismatchByColumn() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS firstBy "
				+ "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
				+ " date INTEGER, \"value\" UNSIGNED_LONG)";
		conn.createStatement().execute(ddl);

		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (1, 8, 5, 8)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (2, 8, 2, 7)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (3, 8, 1, 9)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (4, 8, 4, 4)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (5, 8, 3, 2)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (6, 8, 0, 1)");
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT LAST_BY(\"value\", date, 2) FROM firstBy GROUP BY page_id");

		assertTrue(rs.next());
		assertEquals(rs.getInt(1), 4);
		assertFalse(rs.next());
	}

	@Test
	public void unsignedLong() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS firstBy "
				+ "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
				+ " date DATE, \"value\" UNSIGNED_LONG)";
		conn.createStatement().execute(ddl);

		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") "
				+ "VALUES (1, 8, TO_DATE('2013-01-01 00:00:00'), 300)");

		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") "
				+ "VALUES (2, 8, TO_DATE('2013-01-01 00:01:00'), 7)");

		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") "
				+ "VALUES (3, 8, TO_DATE('2013-01-01 00:02:00'), 9)");

		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") "
				+ "VALUES (4, 8, TO_DATE('2013-01-01 00:03:00'), 4)");

		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") "
				+ "VALUES (5, 8, TO_DATE('2013-01-01 00:04:00'), 2)");

		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") "
				+ "VALUES (6, 8, TO_DATE('2013-01-01 00:05:00'), 150)");
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT LAST_BY(\"value\", date) FROM firstBy GROUP BY page_id");

		assertTrue(rs.next());
		assertEquals(rs.getLong(1), 150);

		assertFalse(rs.next());
	}

	@Test
	public void signedLongAsBigInt() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS firstBy "
				+ "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
				+ " date BIGINT, \"value\" BIGINT)";
		conn.createStatement().execute(ddl);

		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (1, 8, 1, 3)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (2, 8, 2, 7)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (3, 8, 3, 9)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (5, 8, 5, 158)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (4, 8, 4, 5)");
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT FIRST_BY(\"value\", date) FROM firstBy GROUP BY page_id");

		assertTrue(rs.next());
		assertEquals(rs.getLong(1), 3);
		assertFalse(rs.next());
	}

	@Test
	public void testColumnModifier() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS firstBy "
				+ "(id INTEGER NOT NULL, page_id UNSIGNED_LONG NOT NULL,"
				+ " dates BIGINT NOT NULL, \"value\" BIGINT CONSTRAINT pk PRIMARY KEY (id, dates DESC))";
		conn.createStatement().execute(ddl);

		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, dates, \"value\") VALUES (1, 8, 1, 3)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, dates, \"value\") VALUES (2, 8, 2, 7)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, dates, \"value\") VALUES (3, 8, 3, 9)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, dates, \"value\") VALUES (5, 8, 5, 158)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, dates, \"value\") VALUES (4, 8, 4, 5)");
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT FIRST_BY(\"value\", dates) FROM firstBy GROUP BY page_id");

		assertTrue(rs.next());
		assertEquals(rs.getLong(1), 3);
		assertFalse(rs.next());
	}

	@Test
	public void signedInteger() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS firstBy "
				+ "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG, date INTEGER, \"value\" INTEGER)";
		conn.createStatement().execute(ddl);

		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (5, 8, 5, -255)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (1, 8, 1, 3)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (2, 8, 2, 7)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (3, 8, 3, 9)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (4, 8, 4, 4)");
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT LAST_BY(\"value\", date) FROM firstBy GROUP BY page_id");

		assertTrue(rs.next());
		assertEquals(rs.getInt(1), -255);
		assertFalse(rs.next());
	}

	@Test
	public void unsignedInteger() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS firstBy "
				+ "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
				+ " date UNSIGNED_INT, \"value\" UNSIGNED_INT)";
		conn.createStatement().execute(ddl);

		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (1, 8, 1, 3)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (2, 8, 2, 7)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (3, 8, 3, 9)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (5, 8, 4, 2)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (4, 8, 5, 4)");
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT LAST_BY(\"value\", date) FROM firstBy GROUP BY page_id");

		assertTrue(rs.next());
		assertEquals(rs.getInt(1), 4);
		assertFalse(rs.next());
	}

	@Test
	public void testLast() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS firstBy "
				+ "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG, "
				+ "date UNSIGNED_INT, \"value\" UNSIGNED_INT)";
		conn.createStatement().execute(ddl);

		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (1, 8, 1, 3)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (2, 8, 2, 7)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (3, 8, 3, 9)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (5, 8, 4, 2)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (4, 8, 5, 4)");
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT LAST_BY(\"value\", date) FROM firstBy GROUP BY page_id");

		assertTrue(rs.next());
		assertEquals(rs.getInt(1), 4);
		assertFalse(rs.next());
	}

	@Test
	public void doubleDataType() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS firstBy "
				+ "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG, "
				+ "date DOUBLE, \"value\" DOUBLE)";
		conn.createStatement().execute(ddl);

		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (1, 8, 1, 300)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (2, 8, 2, 7)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (3, 8, 3, 9)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (5, 8, 4, 2)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (4, 8, 5, 400)");
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT FIRST_BY(\"value\", date) FROM firstBy GROUP BY page_id");

		assertTrue(rs.next());
		assertEquals("doubles", rs.getDouble(1), 300, 0.00001);
		assertFalse(rs.next());
	}

	@Test
	public void charDatatype() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS firstBy "
				+ "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG, "
				+ "date CHAR(3), \"value\" CHAR(3))";
		conn.createStatement().execute(ddl);

		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (1, 8, '1', '300')");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (2, 8, '2', '7')");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (3, 8, '3', '9')");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (5, 8, '4', '2')");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (4, 8, '5', '400')");
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT LAST_BY(\"value\", date) FROM firstBy GROUP BY page_id");

		assertTrue(rs.next());
		assertEquals(rs.getString(1), "400");
		assertFalse(rs.next());
	}

	@Test
	public void varcharFixedLenghtDatatype() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS firstBy "
				+ "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG, "
				+ "date VARCHAR(3), \"value\" VARCHAR(3))";
		conn.createStatement().execute(ddl);

		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (1, 8, '1', '3')");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (2, 8, '2', '7')");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (3, 8, '3', '9')");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (5, 8, '4', '2')");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (4, 8, '5', '4')");
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT FIRST_BY(\"value\", date) FROM firstBy GROUP BY page_id");

		assertTrue(rs.next());
		assertEquals(rs.getString(1), "3");
		assertFalse(rs.next());
	}

	@Test
	public void varcharVariableLenghtDatatype() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS firstBy "
				+ "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
				+ " date VARCHAR, \"value\" VARCHAR)";
		conn.createStatement().execute(ddl);

		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (1, 8, '1', '3')");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (2, 8, '2', '7')");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (3, 8, '3', '9')");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (5, 8, '4', '2')");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (4, 8, '5', '4')");
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT LAST_BY(\"value\", date) FROM firstBy GROUP BY page_id");

		assertTrue(rs.next());
		assertEquals(rs.getString(1), "4");
		assertFalse(rs.next());
	}

	@Test
	public void floatDataType() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS firstBy "
				+ "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
				+ " date FLOAT, \"value\" FLOAT)";
		conn.createStatement().execute(ddl);

		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (1, 8, 1, 300)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (2, 8, 2, 7)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (3, 8, 3, 9)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (5, 8, 4, 2)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (4, 8, 5, 400)");
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT FIRST_BY(\"value\", date) FROM firstBy GROUP BY page_id");

		assertTrue(rs.next());
		assertEquals(rs.getFloat(1), 300.0, 0.000001);
		assertFalse(rs.next());

	}

	@Test
	public void groupMultipleValues() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS firstBy "
				+ "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
				+ " date UNSIGNED_INT, \"value\" UNSIGNED_INT)";
		conn.createStatement().execute(ddl);

		//first page_id
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (1, 8, 1, 3)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (2, 8, 2, 7)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (3, 8, 3, 9)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (5, 8, 4, 2)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (4, 8, 5, 4)");

		//second page_id
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (11, 9, 1, 3)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (12, 9, 2, 7)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (13, 9, 3, 9)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (15, 9, 4, 2)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (14, 9, 5, 40)");

		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT LAST_BY(\"value\", date) FROM firstBy GROUP BY page_id");

		assertTrue(rs.next());
		assertEquals(rs.getInt(1), 4);

		assertTrue(rs.next());
		assertEquals(rs.getInt(1), 40);
		assertFalse(rs.next());
	}

	@Test
	public void nullValuesInAggregatingColumns() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS firstBy "
				+ "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
				+ " date UNSIGNED_INT, \"value\" UNSIGNED_INT)";
		conn.createStatement().execute(ddl);

		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date) VALUES (1, 8, 1)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date) VALUES (2, 8, 2)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date) VALUES (3, 8, 3)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date) VALUES (5, 8, 4)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date) VALUES (4, 8, 5)");
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT LAST_BY(\"value\", date) FROM firstBy GROUP BY page_id");

		assertTrue(rs.next());
		byte[] nothing = rs.getBytes(1);
		assertTrue(nothing == null);
	}

	@Test
	public void nullValuesInAggregatingColumnsSecond() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS firstBy "
				+ "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
				+ " date UNSIGNED_INT, \"value\" UNSIGNED_INT)";
		conn.createStatement().execute(ddl);

		//first page_id
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date) VALUES (1, 8, 1)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date) VALUES (2, 8, 2)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date) VALUES (3, 8, 3)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date) VALUES (5, 8, 4)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date) VALUES (4, 8, 5)");
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT LAST_BY(\"value\", date) FROM firstBy GROUP BY page_id");

		assertTrue(rs.next());
		byte[] nothing = rs.getBytes(1);
		assertTrue(nothing == null);
	}

	@Test
	public void allColumnsNull() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS firstBy "
				+ "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_LONG,"
				+ " date FLOAT, \"value\" FLOAT)";
		conn.createStatement().execute(ddl);

		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id) VALUES (1, 8)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id) VALUES (2, 8)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id) VALUES (3, 8)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id) VALUES (5, 8)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id) VALUES (4, 8)");
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT FIRST_BY(\"value\", date) FROM firstBy GROUP BY page_id");

		assertTrue(rs.next());
		byte[] nothing = rs.getBytes(1);
		assertTrue(nothing == null);
	}

	@Test
	public void inOrderByClausule() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());

		String ddl = "CREATE TABLE IF NOT EXISTS firstBy "
				+ "(id INTEGER NOT NULL PRIMARY KEY, page_id UNSIGNED_INT,"
				+ " date UNSIGNED_INT, \"value\" UNSIGNED_INT)";
		conn.createStatement().execute(ddl);

		//first page
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (1, 8, 1, 3)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (2, 8, 2, 7)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (3, 8, 3, 9)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (5, 8, 4, 2)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (4, 8, 5, 5)");

		//second page
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (5, 2, 1, 3)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (6, 2, 2, 7)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (7, 2, 3, 9)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (8, 2, 4, 2)");
		conn.createStatement().execute("UPSERT INTO firstBy (id, page_id, date, \"value\") VALUES (9, 2, 5, 4)");

		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT LAST_BY(\"value\", date), page_id FROM firstBy"
				+ " GROUP BY page_id ORDER BY LAST_BY(\"value\", date) DESC");

		assertTrue(rs.next());
		assertEquals(rs.getInt(1), 5);

		assertTrue(rs.next());
		assertEquals(rs.getInt(1), 4);
		assertFalse(rs.next());
	}
}
