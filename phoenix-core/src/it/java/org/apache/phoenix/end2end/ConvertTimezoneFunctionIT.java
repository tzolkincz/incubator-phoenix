package org.apache.phoenix.end2end;

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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import org.junit.Test;

/**
 *
 * @author tzolkincz
 */
public class ConvertTimezoneFunctionIT extends BaseHBaseManagedTimeIT {

	@Test
	public void testConvertTimezoneEurope() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		String ddl = "CREATE TABLE IF NOT EXISTS TIMEZONE_OFFSET_TEST (k1 INTEGER NOT NULL, dates DATE CONSTRAINT pk PRIMARY KEY (k1))";
		conn.createStatement().execute(ddl);
		String dml = "UPSERT INTO TIMEZONE_OFFSET_TEST (k1, dates) VALUES (1, TO_DATE('2014-03-01 00:00:00'))";
		conn.createStatement().execute(dml);
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT k1, dates, CONVERT_TZ(dates, 'UTC', 'Europe/Prague') FROM TIMEZONE_OFFSET_TEST");

		assertTrue(rs.next());
		assertEquals(1393635600000L, rs.getDate(3).getTime());

	}

	@Test
	public void testConvertTimezoneAmerica() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		String ddl = "CREATE TABLE IF NOT EXISTS TIMEZONE_OFFSET_TEST (k1 INTEGER NOT NULL, dates DATE CONSTRAINT pk PRIMARY KEY (k1))";
		conn.createStatement().execute(ddl);
		String dml = "UPSERT INTO TIMEZONE_OFFSET_TEST (k1, dates) VALUES (1, TO_DATE('2014-03-01 00:00:00'))";
		conn.createStatement().execute(dml);
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT k1, dates, CONVERT_TZ(dates, 'UTC', 'America/Adak') FROM TIMEZONE_OFFSET_TEST");

		assertTrue(rs.next());
		assertEquals(1393596000000L, rs.getDate(3).getTime());
	}
}
