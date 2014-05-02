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
package org.apache.phoenix.end2end;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import org.junit.Test;

public class IndexSumIT extends BaseHBaseManagedTimeIT {

	@Test
	public void testIndexSumAggregate() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		
		conn.createStatement().execute("CREATE TABLE tbl (id INTEGER, col INTEGER  CONSTRAINT pk PRIMARY KEY ( id))");
		conn.createStatement().execute("CREATE index tbl_idx ON tbl (col, id)");
		conn.createStatement().execute("UPSERT INTO tbl (id, col) VALUES (1, NULL)");
		conn.commit();
		conn.createStatement().execute("UPSERT INTO tbl (id, col) VALUES (2, 4)");
		conn.commit();
		conn.createStatement().execute("UPSERT INTO tbl (id, col) VALUES (3, 5)");
		conn.commit();

		Thread.sleep(1000); // Ensure data indexed
		ResultSet rs = conn.createStatement().executeQuery("SELECT SUM(col) FROM tbl");
		assertTrue(rs.next());
		assertEquals(9, rs.getInt(1));

	}

}
