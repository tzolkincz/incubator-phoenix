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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.junit.Test;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;


public class ArithmeticQueryIT extends BaseHBaseManagedTimeIT {

    @Test
    public void testDecimalUpsertValue() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String ddl = "CREATE TABLE testDecimalArithmetic" + 
                    "  (pk VARCHAR NOT NULL PRIMARY KEY, " +
                    "col1 DECIMAL(31,0), col2 DECIMAL(5), col3 DECIMAL(5,2), col4 DECIMAL)";
            createTestTable(getUrl(), ddl);
            
            // Test upsert correct values 
            String query = "UPSERT INTO testDecimalArithmetic(pk, col1, col2, col3, col4) VALUES(?,?,?,?,?)";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, "valueOne");
            stmt.setBigDecimal(2, new BigDecimal("123456789123456789"));
            stmt.setBigDecimal(3, new BigDecimal("12345"));
            stmt.setBigDecimal(4, new BigDecimal("12.34"));
            stmt.setBigDecimal(5, new BigDecimal("12345.6789"));
            stmt.execute();
            conn.commit();
            
            query = "SELECT col1, col2, col3, col4 FROM testDecimalArithmetic WHERE pk = 'valueOne'";
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(new BigDecimal("123456789123456789"), rs.getBigDecimal(1));
            assertEquals(new BigDecimal("12345"), rs.getBigDecimal(2));
            assertEquals(new BigDecimal("12.34"), rs.getBigDecimal(3));
            assertEquals(new BigDecimal("12345.6789"), rs.getBigDecimal(4));
            assertFalse(rs.next());
            
            query = "UPSERT INTO testDecimalArithmetic(pk, col1, col2, col3) VALUES(?,?,?,?)";
            stmt = conn.prepareStatement(query);
            stmt.setString(1, "valueTwo");
            stmt.setBigDecimal(2, new BigDecimal("1234567890123456789012345678901.12345"));
            stmt.setBigDecimal(3, new BigDecimal("12345.6789"));
            stmt.setBigDecimal(4, new BigDecimal("123.45678"));
            stmt.execute();
            conn.commit();
            
            query = "SELECT col1, col2, col3 FROM testDecimalArithmetic WHERE pk = 'valueTwo'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(new BigDecimal("1234567890123456789012345678901"), rs.getBigDecimal(1));
            assertEquals(new BigDecimal("12345"), rs.getBigDecimal(2));
            assertEquals(new BigDecimal("123.45"), rs.getBigDecimal(3));
            assertFalse(rs.next());
            
            // Test upsert incorrect values and confirm exceptions would be thrown.
            try {
                query = "UPSERT INTO testDecimalArithmetic(pk, col1, col2, col3) VALUES(?,?,?,?)";
                stmt = conn.prepareStatement(query);
                stmt.setString(1, "badValues");
                // one more than max_precision
                stmt.setBigDecimal(2, new BigDecimal("12345678901234567890123456789012"));
                stmt.setBigDecimal(3, new BigDecimal("12345")); 
                stmt.setBigDecimal(4, new BigDecimal("123.45"));
                stmt.execute();
                conn.commit();
                fail("Should have caught bad values.");
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.DATA_EXCEEDS_MAX_CAPACITY.getErrorCode(),e.getErrorCode());
            }
            try {
                query = "UPSERT INTO testDecimalArithmetic(pk, col1, col2, col3) VALUES(?,?,?,?)";
                stmt = conn.prepareStatement(query);
                stmt.setString(1, "badValues");
                stmt.setBigDecimal(2, new BigDecimal("123456"));
                // Exceeds specified precision by 1
                stmt.setBigDecimal(3, new BigDecimal("123456"));
                stmt.setBigDecimal(4, new BigDecimal("123.45"));
                stmt.execute();
                conn.commit();
                fail("Should have caught bad values.");
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.DATA_EXCEEDS_MAX_CAPACITY.getErrorCode(),e.getErrorCode());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDecimalUpsertSelect() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String ddl = "CREATE TABLE source" + 
                    " (pk VARCHAR NOT NULL PRIMARY KEY, col1 DECIMAL(5,2), col2 DECIMAL(5,1), col3 DECIMAL(5,2), col4 DECIMAL(4,4))";
            createTestTable(getUrl(), ddl);
            ddl = "CREATE TABLE target" + 
                    " (pk VARCHAR NOT NULL PRIMARY KEY, col1 DECIMAL(5,1), col2 DECIMAL(5,2), col3 DECIMAL(4,4))";
            createTestTable(getUrl(), ddl);
            
            String query = "UPSERT INTO source(pk, col1) VALUES(?,?)";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, "1");
            stmt.setBigDecimal(2, new BigDecimal("100.12"));
            stmt.execute();
            conn.commit();
            stmt.setString(1, "2");
            stmt.setBigDecimal(2, new BigDecimal("100.34"));
            stmt.execute();
            conn.commit();
            
            // Evaluated on client side.
            // source and target in different tables, values scheme compatible.
            query = "UPSERT INTO target(pk, col2) SELECT pk, col1 from source";
            stmt = conn.prepareStatement(query);
            stmt.execute();
            conn.commit();
            query = "SELECT col2 FROM target";
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(new BigDecimal("100.12"), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(new BigDecimal("100.34"), rs.getBigDecimal(1));
            assertFalse(rs.next());
            // source and target in different tables, values requires scale chopping.
            query = "UPSERT INTO target(pk, col1) SELECT pk, col1 from source";
            stmt = conn.prepareStatement(query);
            stmt.execute();
            conn.commit();
            query = "SELECT col1 FROM target";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(new BigDecimal("100.1"), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(new BigDecimal("100.3"), rs.getBigDecimal(1));
            assertFalse(rs.next());
            // source and target in different tables, values scheme incompatible.
            try {
                query = "UPSERT INTO target(pk, col3) SELECT pk, col1 from source";
                stmt = conn.prepareStatement(query);
                stmt.execute();
                conn.commit();
                fail("Should have caught bad upsert.");
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.DATA_EXCEEDS_MAX_CAPACITY.getErrorCode(),e.getErrorCode());
            }
            
            // Evaluate on server side.
            conn.setAutoCommit(true);
            // source and target in same table, values scheme compatible.
            query = "UPSERT INTO source(pk, col3) SELECT pk, col1 from source";
            stmt = conn.prepareStatement(query);
            stmt.execute();
            conn.commit();
            query = "SELECT col3 FROM source";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(new BigDecimal("100.12"), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(new BigDecimal("100.34"), rs.getBigDecimal(1));
            assertFalse(rs.next());
            // source and target in same table, values requires scale chopping.
            query = "UPSERT INTO source(pk, col2) SELECT pk, col1 from source";
            stmt = conn.prepareStatement(query);
            stmt.execute();
            conn.commit();
            query = "SELECT col2 FROM source";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(new BigDecimal("100.1"), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(new BigDecimal("100.3"), rs.getBigDecimal(1));
            assertFalse(rs.next());
            // source and target in same table, values scheme incompatible.
            query = "UPSERT INTO source(pk, col4) SELECT pk, col1 from source";
            stmt = conn.prepareStatement(query);
            stmt.execute();
            conn.commit();
            query = "SELECT col4 FROM source";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertNull(rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertNull(rs.getBigDecimal(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDoubleAveraging() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String ddl = "CREATE TABLE testDoubleArithmetic" + 
                    "  (pk VARCHAR NOT NULL PRIMARY KEY, col1 DOUBLE)";
            createTestTable(getUrl(), ddl);
            
            
            String query = "UPSERT INTO testDoubleArithmetic(pk, col1) VALUES('1', 0.00002792783736983)";
            conn.prepareStatement(query).execute();

            query = "UPSERT INTO testDoubleArithmetic(pk, col1) VALUES('2', 0.000006373648125914301)";
            conn.prepareStatement(query).execute();
            conn.commit();
//            CAST (col1 as DECIMAL(2,10) )
            query = "SELECT AVG( CAST (col1 as DECIMAL(10,10)) )  FROM testDoubleArithmetic";
            //query = "SELECT SUM(col1)/COUNT(col1) FROM testDoubleArithmetic";
            PreparedStatement stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            Double result = rs.getDouble(1);
            
            assertEquals(new Double("1.715074274787215E-5"), result);
        } finally {
            conn.close();
        }
    }
    @Test
    public void testDecimalAveraging() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String ddl = "CREATE TABLE testDecimalArithmetic" + 
                    "  (pk VARCHAR NOT NULL PRIMARY KEY, col1 DECIMAL(31, 11), col2 DECIMAL(31,1), col3 DECIMAL(38,1))";
            createTestTable(getUrl(), ddl);
            
            String query = "UPSERT INTO testDecimalArithmetic(pk, col1, col2, col3) VALUES(?,?,?,?)";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, "1");
            stmt.setBigDecimal(2, new BigDecimal("99999999999999999999.1"));
            stmt.setBigDecimal(3, new BigDecimal("99999999999999999999.1"));
            stmt.setBigDecimal(4, new BigDecimal("9999999999999999999999999999999999999.1"));
            stmt.execute();
            conn.commit();
            stmt.setString(1, "2");
            stmt.setBigDecimal(2, new BigDecimal("0"));
            stmt.setBigDecimal(3, new BigDecimal("0"));
            stmt.setBigDecimal(4, new BigDecimal("0"));
            stmt.execute();
            conn.commit();
            stmt.setString(1, "3");
            stmt.setBigDecimal(2, new BigDecimal("0"));
            stmt.setBigDecimal(3, new BigDecimal("0"));
            stmt.setBigDecimal(4, new BigDecimal("0"));
            stmt.execute();
            conn.commit();
            
            // Averaging
            // result scale should be: max(max(ls, rs), 4).
            // We are not imposing restriction on precisioin.
            query = "SELECT avg(col1) FROM testDecimalArithmetic";
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            BigDecimal result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("33333333333333333333.03333333333"), result);
            
            query = "SELECT avg(col2) FROM testDecimalArithmetic";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("33333333333333333333.0333"), result);
            
            // We cap our decimal to a precision of 38.
            query = "SELECT avg(col3) FROM testDecimalArithmetic";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("3333333333333333333333333333333333333"), result);
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDecimalArithmeticWithIntAndLong() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String ddl = "CREATE TABLE testDecimalArithmetic" + 
                    "  (pk VARCHAR NOT NULL PRIMARY KEY, " +
                    "col1 DECIMAL(38,0), col2 DECIMAL(5, 2), col3 INTEGER, col4 BIGINT, col5 DECIMAL)";
            createTestTable(getUrl(), ddl);
            
            String query = "UPSERT INTO testDecimalArithmetic(pk, col1, col2, col3, col4, col5) VALUES(?,?,?,?,?,?)";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, "testValueOne");
            stmt.setBigDecimal(2, new BigDecimal("1234567890123456789012345678901"));
            stmt.setBigDecimal(3, new BigDecimal("123.45"));
            stmt.setInt(4, 10);
            stmt.setLong(5, 10L);
            stmt.setBigDecimal(6, new BigDecimal("111.111"));
            stmt.execute();
            conn.commit();

            stmt.setString(1, "testValueTwo");
            stmt.setBigDecimal(2, new BigDecimal("12345678901234567890123456789012345678"));
            stmt.setBigDecimal(3, new BigDecimal("123.45"));
            stmt.setInt(4, 10);
            stmt.setLong(5, 10L);
            stmt.setBigDecimal(6, new BigDecimal("123456789.0123456789"));
            stmt.execute();
            conn.commit();
            
            // INT has a default precision and scale of (10, 0)
            // LONG has a default precision and scale of (19, 0)
            query = "SELECT col1 + col3 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            BigDecimal result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1234567890123456789012345678911"), result);
            
            query = "SELECT col1 + col4 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1234567890123456789012345678911"), result);
            
            query = "SELECT col2 + col3 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("133.45"), result);
            
            query = "SELECT col2 + col4 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("133.45"), result);
            
            query = "SELECT col5 + col3 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("121.111"), result);
            
            query = "SELECT col5 + col4 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("121.111"), result);
            
            query = "SELECT col1 - col3 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1234567890123456789012345678891"), result);
            
            query = "SELECT col1 - col4 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1234567890123456789012345678891"), result);
            
            query = "SELECT col2 - col3 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("113.45"), result);
            
            query = "SELECT col2 - col4 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("113.45"), result);
            
            query = "SELECT col5 - col3 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("101.111"), result);
            
            query = "SELECT col5 - col4 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("101.111"), result);
            
            query = "SELECT col1 * col3 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1.234567890123456789012345678901E+31"), result);
            
            query = "SELECT col1 * col4 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1.234567890123456789012345678901E+31"), result);

            query = "SELECT col1 * col3 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1.234567890123456789012345678901E+31"), result);
            
            try {
            	query = "SELECT col1 * col3 FROM testDecimalArithmetic WHERE pk='testValueTwo'";
            	stmt = conn.prepareStatement(query);
            	rs = stmt.executeQuery();
            	assertTrue(rs.next());
            	result = rs.getBigDecimal(1);
            	fail("Should have caught error.");
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.DATA_EXCEEDS_MAX_CAPACITY.getErrorCode(),e.getErrorCode());
            }
            
            try {
            	query = "SELECT col1 * col4 FROM testDecimalArithmetic WHERE pk='testValueTwo'";
            	stmt = conn.prepareStatement(query);
            	rs = stmt.executeQuery();
            	assertTrue(rs.next());
            	result = rs.getBigDecimal(1);
            	fail("Should have caught error.");
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.DATA_EXCEEDS_MAX_CAPACITY.getErrorCode(),e.getErrorCode());
            }
            
            query = "SELECT col4 * col5 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(0, result.compareTo(new BigDecimal("1111.11")));

            query = "SELECT col3 * col5 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(0, result.compareTo(new BigDecimal("1111.11")));
            
            query = "SELECT col2 * col4 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1234.5"), result);
            
            // Result scale has value of 0
            query = "SELECT col1 / col3 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1.2345678901234567890123456789E+29"), result);
            
            query = "SELECT col1 / col4 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1.2345678901234567890123456789E+29"), result);
            
            // Result scale is 2.
            query = "SELECT col2 / col3 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("12.34"), result);
            
            query = "SELECT col2 / col4 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("12.34"), result);
            
            // col5 has NO_SCALE, so the result's scale is not expected to be truncated to col5 value's scale of 4
            query = "SELECT col5 / col3 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("11.1111"), result);
            
            query = "SELECT col5 / col4 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("11.1111"), result);
        } finally {
            conn.close();
        }
    }
    @Test
    public void testSumDouble() throws Exception {
        initSumDoubleValues(null);
        String query = "SELECT SUM(d) FROM SumDoubleTest";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertTrue(Doubles.compare(rs.getDouble(1), 0.015)==0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSumUnsignedDouble() throws Exception {
        initSumDoubleValues(null);
        String query = "SELECT SUM(ud) FROM SumDoubleTest";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertTrue(Doubles.compare(rs.getDouble(1), 0.015)==0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSumFloat() throws Exception {
        initSumDoubleValues(null);
        String query = "SELECT SUM(f) FROM SumDoubleTest";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertTrue(Floats.compare(rs.getFloat(1), 0.15f)==0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSumUnsignedFloat() throws Exception {
        initSumDoubleValues(null);
        String query = "SELECT SUM(uf) FROM SumDoubleTest";
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertTrue(Floats.compare(rs.getFloat(1), 0.15f)==0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
}