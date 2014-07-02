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
package org.apache.phoenix.jdbc;

import static org.apache.phoenix.util.PhoenixRuntime.PHOENIX_TEST_DRIVER_URL_PARAM;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SQLCloseable;

import com.google.common.collect.Maps;



/**
 * 
 * Abstract base class for JDBC Driver implementation of Phoenix
 * 
 * 
 * @since 0.1
 */
public abstract class PhoenixEmbeddedDriver implements Driver, org.apache.phoenix.jdbc.Jdbc7Shim.Driver, SQLCloseable {
    /**
     * The protocol for Phoenix Network Client 
     */ 
    private final static String DNC_JDBC_PROTOCOL_SUFFIX = "//";
    private static final String TERMINATOR = "" + PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
    private static final String DELIMITERS = TERMINATOR + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
    private static final String TEST_URL_AT_END =  "" + PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR + PHOENIX_TEST_DRIVER_URL_PARAM;
    private static final String TEST_URL_IN_MIDDLE = TEST_URL_AT_END + PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;

    private final static DriverPropertyInfo[] EMPTY_INFO = new DriverPropertyInfo[0];
    public final static String MAJOR_VERSION_PROP = "DriverMajorVersion";
    public final static String MINOR_VERSION_PROP = "DriverMinorVersion";
    public final static String DRIVER_NAME_PROP = "DriverName";
    
    PhoenixEmbeddedDriver() {
    }
    
    private String getDriverName() {
        return this.getClass().getName();
    }
    
    abstract public QueryServices getQueryServices();
     
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (url.startsWith(PhoenixRuntime.JDBC_PROTOCOL)) {
            // A connection string of "jdbc:phoenix" is supported, since
            // all the connection information can potentially be gotten
            // out of the HBase config file
            if (url.length() == PhoenixRuntime.JDBC_PROTOCOL.length()) {
                return true;
            }
            // Same as above, except for "jdbc:phoenix;prop=<value>..."
            if (PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR == url.charAt(PhoenixRuntime.JDBC_PROTOCOL.length())) {
                return true;
            }
            if (PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR == url.charAt(PhoenixRuntime.JDBC_PROTOCOL.length())) {
                int protoLength = PhoenixRuntime.JDBC_PROTOCOL.length() + 1;
                // A connection string of "jdbc:phoenix:" matches this driver,
                // but will end up as a MALFORMED_CONNECTION_URL exception later.
                if (url.length() == protoLength) {
                    return true;
                }
                // A connection string of the form "jdbc:phoenix://" means that
                // the driver is remote which isn't supported, so return false.
                if (!url.startsWith(DNC_JDBC_PROTOCOL_SUFFIX, protoLength)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        ConnectionQueryServices connectionServices = getConnectionQueryServices(url, info);
        info.setProperty(MAJOR_VERSION_PROP, Integer.toString(getMajorVersion()));
        info.setProperty(MINOR_VERSION_PROP, Integer.toString(getMinorVersion()));
        info.setProperty(DRIVER_NAME_PROP, getDriverName());
        PhoenixConnection connection = connectionServices.connect(url, info);
        return connection;
    }

    /**
     * Get or create if necessary a QueryServices that is associated with the HBase zookeeper quorum
     * name (part of the connection URL). This will cause the underlying Configuration held by the
     * QueryServices to be shared for all connections to the same HBase cluster.
     * @param url connection URL
     * @param info connection properties
     * @return new or cached QuerySerices used to establish a new Connection.
     * @throws SQLException
     */
    protected abstract ConnectionQueryServices getConnectionQueryServices(String url, Properties info) throws SQLException;
    
    @Override
    public int getMajorVersion() {
        return MetaDataProtocol.PHOENIX_MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return MetaDataProtocol.PHOENIX_MINOR_VERSION;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return EMPTY_INFO;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    @Override
    public void close() throws SQLException {
    }
    
    /**
     * 
     * Class to encapsulate connection info for HBase
     *
     * 
     * @since 0.1.1
     */
    public static class ConnectionInfo {
        protected static ConnectionInfo create(String url) throws SQLException {
            StringTokenizer tokenizer = new StringTokenizer(url == null ? "" : url.substring(PhoenixRuntime.JDBC_PROTOCOL.length()),DELIMITERS, true);
            int i = 0;
            boolean isMalformedUrl = false;
            String[] tokens = new String[5];
            String token = null;
            while (tokenizer.hasMoreTokens() && !(token=tokenizer.nextToken()).equals(TERMINATOR) && tokenizer.hasMoreTokens() && i < tokens.length) {
                token = tokenizer.nextToken();
                // This would mean we have an empty string for a token which is illegal
                if (DELIMITERS.contains(token)) {
                    isMalformedUrl = true;
                    break;
                }
                tokens[i++] = token;
            }
            Integer port = null;
            if (!isMalformedUrl) {
                if (tokenizer.hasMoreTokens() && !TERMINATOR.equals(token)) {
                    isMalformedUrl = true;
                } else if (i > 1) {
                    try {
                        port = Integer.parseInt(tokens[1]);
                        isMalformedUrl = port < 0;
                        if(i == 4){
                        	if(!tokens[2].endsWith(".keytab")){
                        		isMalformedUrl = true;
                        	}
                        	tokens[4] = tokens[3];
                        	tokens[3] = tokens[2];
                        	tokens[2] = null;
                        }
                    } catch (NumberFormatException e) {
                        // If we have 3 tokens, then the second one must be a port.
                        // If we only have 2 tokens, the second one might be the root node:
                        // Assume that is the case if we get a NumberFormatException
                        if (!tokens[1].startsWith("/")) {
                            isMalformedUrl = true;
                        }
                        if (i == 2) {
                            tokens[4] = null;
                            tokens[3] = null;
                            tokens[2] = tokens[1];
                            tokens[1] = null;
                        } else if (i == 3) {
                            tokens[4] = tokens[2];
                            tokens[3] = tokens[1];
                            tokens[2] = null;
                            tokens[1] = null;
                        } else if (i == 4) {
                            tokens[4] = tokens[3];
                            tokens[3] = tokens[2];
                            tokens[2] = tokens[1];
                            tokens[1] = null;
                        } else if (i == 5) {
                            tokens[4] = tokens[3];
                            tokens[3] = tokens[2];
                            tokens[2] = tokens[1];
                        }
                    }
                }
            }
            if (isMalformedUrl) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.MALFORMED_CONNECTION_URL)
                .setMessage(url).build().buildException();
            }
            return new ConnectionInfo(tokens[0],port,tokens[2], tokens[3], tokens[4]);
        }
        
        public ConnectionInfo normalize(ReadOnlyProps props) throws SQLException {
            String zookeeperQuorum = this.getZookeeperQuorum();
            Integer port = this.getPort();
            String rootNode = this.getRootNode();
            String keytab = this.getKeytab();
            String principal = this.getPrincipal();
            // Normalize connInfo so that a url explicitly specifying versus implicitly inheriting
            // the default values will both share the same ConnectionQueryServices.
            if (zookeeperQuorum == null) {
                zookeeperQuorum = props.get(QueryServices.ZOOKEEPER_QUARUM_ATTRIB);
                if (zookeeperQuorum == null) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.MALFORMED_CONNECTION_URL)
                    .setMessage(this.toString()).build().buildException();
                }
            }

            if (port == null) {
                if (!isConnectionless) {
                    String portStr = props.get(QueryServices.ZOOKEEPER_PORT_ATTRIB);
                    if (portStr != null) {
                        try {
                            port = Integer.parseInt(portStr);
                        } catch (NumberFormatException e) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.MALFORMED_CONNECTION_URL)
                            .setMessage(this.toString()).build().buildException();
                        }
                    }
                }
            } else if (isConnectionless) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.MALFORMED_CONNECTION_URL)
                .setMessage("Port may not be specified when using the connectionless url \"" + this.toString() + "\"").build().buildException();
            }
            if (rootNode == null) {
                if (!isConnectionless) {
                    rootNode = props.get(QueryServices.ZOOKEEPER_ROOT_NODE_ATTRIB);
                }
            } else if (isConnectionless) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.MALFORMED_CONNECTION_URL)
                .setMessage("Root node may not be specified when using the connectionless url \"" + this.toString() + "\"").build().buildException();
            }
            if(keytab == null){
            	 if (!isConnectionless) {
            		 keytab = props.get(QueryServices.HBASE_CLIENT_KEYTAB);
            	 }
            }
            if(principal == null){
              	 if (!isConnectionless) {
              		principal = props.get(QueryServices.HBASE_CLIENT_PRINCIPAL);
              	 }
              }
            if (keytab == null || keytab.equals("")) return new ConnectionInfo(zookeeperQuorum,
                    port, rootNode);
            else return new ConnectionInfo(zookeeperQuorum, port, rootNode, keytab, principal);
        }
        
        private final Integer port;
        private final String rootNode;
        private final String zookeeperQuorum;
        private final boolean isConnectionless;
        private final String principal;
        private final String keytab;
        
        // used for testing
        ConnectionInfo(String zookeeperQuorum, Integer port, String rootNode, String keytab, String principal) {
            this.zookeeperQuorum = zookeeperQuorum;
            this.port = port;
            this.rootNode = rootNode;
            this.isConnectionless = PhoenixRuntime.CONNECTIONLESS.equals(zookeeperQuorum);
            this.principal = principal;
            this.keytab = keytab;
        }
        
        // used for testing
        ConnectionInfo(String zookeeperQuorum, Integer port, String rootNode) {
        	this(zookeeperQuorum, port, rootNode, null, null);
        }

        public ReadOnlyProps asProps() {
            Map<String, String> connectionProps = Maps.newHashMapWithExpectedSize(3);
            if (getZookeeperQuorum() != null) {
                connectionProps.put(QueryServices.ZOOKEEPER_QUARUM_ATTRIB, getZookeeperQuorum());
            }
            if (getPort() != null) {
                connectionProps.put(QueryServices.ZOOKEEPER_PORT_ATTRIB, getPort().toString());
            }
            if (getRootNode() != null) {
                connectionProps.put(QueryServices.ZOOKEEPER_ROOT_NODE_ATTRIB, getRootNode());
            }
            if (getKeytab() != null) {
                connectionProps.put(QueryServices.HBASE_CLIENT_KEYTAB, getKeytab());
            }
            if (getPrincipal() != null) {
                connectionProps.put(QueryServices.HBASE_CLIENT_PRINCIPAL, getPrincipal());
            }
            return connectionProps.isEmpty() ? ReadOnlyProps.EMPTY_PROPS : new ReadOnlyProps(
                    connectionProps.entrySet().iterator());
        }
        
        public boolean isConnectionless() {
            return isConnectionless;
        }
        
        public String getZookeeperQuorum() {
            return zookeeperQuorum;
        }

        public Integer getPort() {
            return port;
        }

        public String getRootNode() {
            return rootNode;
        }
        
        public String getKeytab() {
            return keytab;
        }

        public String getPrincipal() {
            return principal;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((zookeeperQuorum == null) ? 0 : zookeeperQuorum.hashCode());
            result = prime * result + ((port == null) ? 0 : port.hashCode());
            result = prime * result + ((rootNode == null) ? 0 : rootNode.hashCode());
            result = prime * result + ((keytab == null) ? 0 : keytab.hashCode());
            result = prime * result + ((principal == null) ? 0 : keytab.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            ConnectionInfo other = (ConnectionInfo) obj;
            if (zookeeperQuorum == null) {
                if (other.zookeeperQuorum != null) return false;
            } else if (!zookeeperQuorum.equals(other.zookeeperQuorum)) return false;
            if (port == null) {
                if (other.port != null) return false;
            } else if (!port.equals(other.port)) return false;
            if (rootNode == null) {
                if (other.rootNode != null) return false;
            } else if (!rootNode.equals(other.rootNode)) return false;
            if (keytab == null) {
                if (other.keytab != null) return false;
            } else if (!keytab.equals(other.keytab)) return false;
            if (principal == null) {
                if (other.principal != null) return false;
            } else if (!principal.equals(other.principal)) return false;
            return true;
        }
        
        @Override
		public String toString() {
			return zookeeperQuorum + (port == null ? "" : ":" + port)
					+ (rootNode == null ? "" : ":" + rootNode)
					+ (keytab == null ? "" : ":" + keytab)
					+ (principal == null ? "" : ":" + principal);
		}
    }

    public static boolean isTestUrl(String url) {
        return url.endsWith(TEST_URL_AT_END) || url.contains(TEST_URL_IN_MIDDLE);
    }
}
