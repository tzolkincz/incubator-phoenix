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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.PDataType;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 *
 * @author tzolkincz
 */
public class HexToBytesFunctionIT extends BaseHBaseManagedTimeIT {

    @Test
    public void shouldPass() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());

        String ddl = "CREATE TABLE test_table ( page_offset_id BINARY(12) NOT NULL CONSTRAINT PK PRIMARY KEY (page_offset_id))";

        conn.createStatement().execute(ddl);
        PreparedStatement ps = conn.prepareStatement("UPSERT INTO test_table (page_offset_id) VALUES (?)");

        byte[] kk = Bytes.add(PDataType.UNSIGNED_LONG.toBytes(2232594215l), PDataType.INTEGER.toBytes(-8));
        ps.setBytes(1, kk);

        ps.execute();
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM test_table WHERE page_offset_id = HEX_TO_BYTES('000000008512af277ffffff8')");

        assertTrue(rs.next());
    }

    @Test
    public void shouldFail() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE test_table ( page_offset_id BINARY(12) NOT NULL CONSTRAINT PK PRIMARY KEY (page_offset_id))";

        conn.createStatement().execute(ddl);
        PreparedStatement ps = conn.prepareStatement("UPSERT INTO test_table (page_offset_id) VALUES (?)");

        byte[] kk = Bytes.add(PDataType.UNSIGNED_LONG.toBytes(2232594215l), PDataType.INTEGER.toBytes(-9));
        ps.setBytes(1, kk);

        ps.execute();
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM test_table WHERE page_offset_id = HEX_TO_BYTES('000000008512af277ffffff8')");

        assertFalse(rs.next());
    }

    @Test
    public void invalidCharacters() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE test_table ( page_offset_id BINARY(12) NOT NULL CONSTRAINT PK PRIMARY KEY (page_offset_id))";

        conn.createStatement().execute(ddl);

        try {
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM test_table WHERE page_offset_id = HEX_TO_BYTES('zzxxuuyyzzxxuuyy')");
        } catch (IllegalDataException e) {
            assertTrue(true);
            return;
        }
        fail();
    }

    @Test
    public void invalidLenght() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE test_table ( page_offset_id BINARY(12) NOT NULL CONSTRAINT PK PRIMARY KEY (page_offset_id))";

        conn.createStatement().execute(ddl);

        try {
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM test_table WHERE page_offset_id = HEX_TO_BYTES('8')");
        } catch (IllegalDataException e) {
            assertTrue(true);
            return;
        }
        fail();
    }
}
