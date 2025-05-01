/*
   Copyright (c) 2010, 2025, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

package com.mysql.clusterj.core;

import com.mysql.clusterj.ClusterJDatastoreException;
import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.ClusterJFatalException;
import com.mysql.clusterj.ClusterJFatalInternalException;
import com.mysql.clusterj.ClusterJFatalUserException;
import com.mysql.clusterj.ClusterJUserException;
import com.mysql.clusterj.Connection;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;
import com.mysql.clusterj.core.spi.DomainTypeHandler;
import com.mysql.clusterj.core.spi.DomainTypeHandlerFactory;
import com.mysql.clusterj.core.spi.ValueHandlerFactory;
import com.mysql.clusterj.core.metadata.DomainTypeHandlerFactoryImpl;

import com.mysql.clusterj.core.store.Db;
import com.mysql.clusterj.core.store.DbFactory;
import com.mysql.clusterj.core.store.ConnectionHandle;
import com.mysql.clusterj.core.store.Dictionary;
import com.mysql.clusterj.core.store.Table;

import com.mysql.clusterj.core.util.I18NHelper;
import com.mysql.clusterj.core.util.Logger;
import com.mysql.clusterj.core.util.LoggerFactoryService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SessionFactoryImpl implements SessionFactory {

    /** My message translator */
    static final I18NHelper local = I18NHelper.getInstance(SessionFactoryImpl.class);

    /** My logger */
    static final Logger logger = LoggerFactoryService.getFactory().getInstance(SessionFactoryImpl.class);

    /** My class loader */
    static final ClassLoader SESSION_FACTORY_IMPL_CLASS_LOADER = SessionFactoryImpl.class.getClassLoader();

    /** The status of this session factory */
    protected State state;

    /** The properties */
    private final Map<?, ?> props;

    /** NdbCluster connect properties */
    private static class Spec extends PropertyReader {
        final int CONNECTION_POOL_SIZE;
        final String CONNECT_STRING;
        final String DATABASE;
        final int MAX_TRANSACTIONS;
        final int RECONNECT_TIMEOUT;
        final int RECV_THREAD_ACTIVATION_THRESHOLD;

        Spec(Map<?, ?> props) {
            CONNECTION_POOL_SIZE = getIntProperty(props, PROPERTY_CONNECTION_POOL_SIZE,
                                                  DEFAULT_PROPERTY_CONNECTION_POOL_SIZE);
            CONNECT_STRING = getRequiredStringProperty(props, PROPERTY_CLUSTER_CONNECTSTRING);
            DATABASE = getStringProperty(props, PROPERTY_CLUSTER_DATABASE,
                                         DEFAULT_PROPERTY_CLUSTER_DATABASE);
            MAX_TRANSACTIONS = getIntProperty(props, PROPERTY_CLUSTER_MAX_TRANSACTIONS,
                                              DEFAULT_PROPERTY_CLUSTER_MAX_TRANSACTIONS);
            RECONNECT_TIMEOUT = getIntProperty(props, PROPERTY_CONNECTION_RECONNECT_TIMEOUT,
                                               DEFAULT_PROPERTY_CONNECTION_RECONNECT_TIMEOUT);
            RECV_THREAD_ACTIVATION_THRESHOLD = getIntProperty(props, PROPERTY_CONNECTION_POOL_RECV_THREAD_ACTIVATION_THRESHOLD,
                                                              DEFAULT_PROPERTY_CONNECTION_POOL_RECV_THREAD_ACTIVATION_THRESHOLD);
        }
    }

    private final Spec spec;
    private int CLUSTER_RECV_THREAD_ACTIVATION_THRESHOLD = 0;
    private int CLUSTER_RECONNECT_TIMEOUT = 0;

    /** Node ids obtained from the property PROPERTY_CONNECTION_POOL_NODEIDS */
    List<Integer> nodeIds = new ArrayList<Integer>();

    /** Actual number of connection handles obtained from the global connection pool */
    int connectionPoolSize;

    /** Internal class of one-per-connection data members.
    */
    static class PooledConnection {
         final ConnectionHandle connection;
         final DbFactory dbFactory;

        PooledConnection(ConnectionHandle c, String database) {
            connection = c;
            dbFactory = connection.createDbFactory(database);
        }

        Db createDb(int maxTransactions) {
            return dbFactory.createDb(maxTransactions);
        }

        ConnectionHandle handle()         { return connection; }

        State currentState()              { return connection.currentState(); }

        void unloadSchema(String table)   { dbFactory.unloadSchema(table); }

        int dbCount()                     { return dbFactory.dbCount(); }

        ValueHandlerFactory getSmartValueHandlerFactory() {
            return connection.getSmartValueHandlerFactory();
        }

        void setClosing()                 { dbFactory.closing(); }

        void close() {
            dbFactory.close();
            connection.close();
        }

        void reconnect(int timeout)       { connection.reconnect(timeout); }

        void setRecvThreadCPUid(short id) { connection.setRecvThreadCPUid(id); }

        void unsetRecvThreadCPUid()       { setRecvThreadCPUid((short)-1); }

        void setRecvThreadActivationThreshold(int t) {
            connection.setRecvThreadActivationThreshold(t);
        }

        boolean isReconnecting() {
            return connection.currentState().equals(State.Reconnecting);
        }
    }

    /** Boolean flag indicating if connection pool is disabled or not */
    boolean connectionPoolDisabled = false;

    /** Map of Proxy to Class */
    static private Map<Class<?>, Class<?>> proxyClassToDomainClass =
            new ConcurrentHashMap<>();

    /** Map of Domain Class to DomainTypeHandler. */
    final private Map<Class<?>, DomainTypeHandler<?>> typeToHandlerMap =
            new HashMap<Class<?>, DomainTypeHandler<?>>();

    /** DomainTypeHandlerFactory for this session factory. */
    DomainTypeHandlerFactory domainTypeHandlerFactory = new DomainTypeHandlerFactoryImpl();

    /** The session factories. */
    static final protected Map<String, SessionFactoryImpl> sessionFactoryMap =
            new HashMap<String, SessionFactoryImpl>();

    /** The key for this factory */
    final private String key;

    /** Cluster connections that together can be used to manage sessions */
    private List<PooledConnection> pooledConnections = new ArrayList<PooledConnection>();

    /** The smart value handler factory */
    protected ValueHandlerFactory smartValueHandlerFactory;

    /** The cpuids to which the receive threads of the connections in the connection pools are locked */
    short[] recvThreadCPUids;

    /** Get a session factory. If using connection pooling and there is already a session factory
     * with the same connect string and database, return it, regardless of whether other
     * properties of the factory are the same as specified in the Map.
     * If not using connection pooling (maximum sessions per connection == 0), create a new session factory.
     * @param props properties of the session factory
     * @return the session factory
     */
    static public SessionFactoryImpl getSessionFactory(Map<?, ?> props) {
        SessionFactoryImpl result = null;
        Spec spec = new Spec(props);

        if(spec.CONNECTION_POOL_SIZE > 0) {
            String sessionFactoryKey = getSessionFactoryKey(spec);
            synchronized(sessionFactoryMap) {
                result = sessionFactoryMap.get(sessionFactoryKey);
                if (result == null) {
                    result = new SessionFactoryImpl(spec, props);
                    sessionFactoryMap.put(sessionFactoryKey, result);
                }
            }
        } else {
            // if not using connection pooling, create a new session factory
            result = new SessionFactoryImpl(spec, props);
        }
        return result;
    }

    private static String getSessionFactoryKey(Spec spec) {
        return spec.CONNECT_STRING + "+" + spec.DATABASE;
    }

    /* Returns a ConnectionHandle to SessionImpl for session.getConnection() */
    protected ConnectionHandle getConnectionHandle(int index) {
        return pooledConnections.get(index).handle();
    }

    /** Create a new SessionFactoryImpl from the properties in the Map, and
     * connect to the ndb cluster.
     *
     * @param props the properties for the factory
     */
    private SessionFactoryImpl(Spec spec, Map<?, ?> props) {
        this.spec = spec;
        this.props = props;
        key = getSessionFactoryKey(spec);
        CLUSTER_RECV_THREAD_ACTIVATION_THRESHOLD = spec.RECV_THREAD_ACTIVATION_THRESHOLD;
        CLUSTER_RECONNECT_TIMEOUT = spec.RECONNECT_TIMEOUT;
        connectionPoolSize = createClusterConnectionPool();
        verifyConnectionPool();
        state = State.Open;
        GlobalConnectionPool.registerSessionFactory(spec.CONNECT_STRING, this);
    }

    private int createClusterConnectionPool() {
        List<ConnectionHandle> handles = new ArrayList<ConnectionHandle>();

        // Pass props to global pool to obtain a list of connection handles
        GlobalConnectionPool.getConnections(handles, props);

       // Move the handles into PooledConnections
        for(ConnectionHandle handle : handles)
            pooledConnections.add(new PooledConnection(handle, spec.DATABASE));

        // get the smart value handler factory (it will be the same for all connections)
        smartValueHandlerFactory = pooledConnections.get(0).getSmartValueHandlerFactory();

        return pooledConnections.size();
    }

    protected void verifyConnectionPool() {
        assert connectionPoolSize > 0;
        // Get a Session for each connection in the pool and complete
        // a transaction to make sure that each connection is ready
        List<Integer> sessionCounts = null;
        String msg;
        try {
            List<Session> sessions = new ArrayList<Session>(pooledConnections.size());
            for (int i = 0; i < pooledConnections.size(); ++i) {
                sessions.add(getSession(null, true));
            }
            sessionCounts = getConnectionPoolSessionCounts();
            for (Session session: sessions) {
                session.currentTransaction().begin();
                session.currentTransaction().commit();
                session.close();
            }
        } catch (RuntimeException e) {
            msg = local.message("ERR_Session_Factory_Impl_Failed_To_Complete_Transaction");
            logger.warn(msg);
            throw e;
        }
        // verify that the session counts were correct
        for (Integer count: sessionCounts) {
            if (count != 1) {
                msg = local.message("ERR_Session_Counts_Wrong_Creating_Factory",
                        sessionCounts.toString());
                logger.warn(msg);
                throw new ClusterJFatalInternalException(msg);
            }
        }
    }

    /** Get a session to use with the cluster.
     *
     * @return the session
     */
    public Session getSession() {
        return getSession(null, false);
    }

    public Session getSession(Map properties) {
        return getSession(null, false);
    }

    /** Get a session to use with the cluster, overriding some properties.
     * Properties PROPERTY_CLUSTER_CONNECTSTRING, PROPERTY_CLUSTER_DATABASE,
     * and PROPERTY_CLUSTER_MAX_TRANSACTIONS may not be overridden.
     * @param properties overriding some properties for this session
     * @return the session
     */
    public Session getSession(Map properties, boolean internal) {
        try {
            Db db = null;
            int idx = 0;
            synchronized(this) {
                if (!(State.Open.equals(state)) && !internal) {
                    throw new ClusterJUserException(local.message("ERR_SessionFactory_not_open"));
                }
                idx = getIndexOfBestPooledConnection();
                PooledConnection connection = pooledConnections.get(idx);
                checkConnection(connection);
                db = connection.createDb(spec.MAX_TRANSACTIONS);
            }
            return new SessionImpl(this, idx, db);
        } catch (ClusterJException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ClusterJFatalException(
                    local.message("ERR_Create_Ndb"), ex);
        }
    }

    private int getIndexOfBestPooledConnection() {
        int result = 0;
        if (connectionPoolSize == 1) {
            return result;
        }
        // find the best pooled connection (the connection with the least active sessions)
        // this is not perfect without synchronization since a connection might close sessions
        // after getting the dbCount but we don't care about perfection here. 
        int bestCount = Integer.MAX_VALUE;
        for (int i = 0 ; i < connectionPoolSize ; i++) {
            PooledConnection connection = pooledConnections.get(i);
            int count = connection.dbCount();
            if (count < bestCount) {
                bestCount = count;
                result = i;
            }
        }
        return result;
    }

    private void checkConnection(PooledConnection connection) {
        if (connection == null) {
            throw new ClusterJUserException(local.message("ERR_Session_Factory_Closed"));
        }
    }

    /** Get the DomainTypeHandler for a class. If the handler is not already
     * available, null is returned. 
     * @param cls the Class for which to get domain type handler
     * @return the DomainTypeHandler or null if not available
     */
    <T> DomainTypeHandler<T> getDomainTypeHandler(Class<T> cls) {
        // synchronize here because the map is not synchronized
        synchronized(typeToHandlerMap) {
            @SuppressWarnings( "unchecked" )
            DomainTypeHandler<T> domainTypeHandler = (DomainTypeHandler<T>) typeToHandlerMap.get(cls);
            return domainTypeHandler;
        }
    }

    /** Create or get the DomainTypeHandler for a class.
     * Use the dictionary to validate against schema.
     * @param cls the Class for which to get domain type handler
     * @param dictionary the dictionary to validate against
     * @return the type handler
     */
    public <T> DomainTypeHandler<T> getDomainTypeHandler(Class<T> cls, Dictionary dictionary) {
        // synchronize here because the map is not synchronized
        synchronized(typeToHandlerMap) {
            @SuppressWarnings("unchecked")
            DomainTypeHandler<T> domainTypeHandler = (DomainTypeHandler<T>) typeToHandlerMap.get(cls);
            if (logger.isDetailEnabled()) logger.detail("DomainTypeToHandler for "
                    + cls.getName() + "(" + cls + ") returned " + domainTypeHandler);
            if (domainTypeHandler == null) {
                domainTypeHandler = domainTypeHandlerFactory.createDomainTypeHandler(cls,
                        dictionary, smartValueHandlerFactory);
                if (logger.isDetailEnabled()) logger.detail("createDomainTypeHandler for "
                        + cls.getName() + "(" + cls + ") returned " + domainTypeHandler);
                typeToHandlerMap.put(cls, domainTypeHandler);
                Class<?> proxyClass = domainTypeHandler.getProxyClass();
                if (proxyClass != null) {
                    proxyClassToDomainClass.put(proxyClass, cls);
                }
            }
            return domainTypeHandler;
        }
    }

    /** Create or get the DomainTypeHandler for an instance.
     * Use the dictionary to validate against schema.
     * @param object the object
     * @param dictionary the dictionary for metadata access
     * @return the DomainTypeHandler for the object
     */
    <T> DomainTypeHandler<T> getDomainTypeHandler(T object, Dictionary dictionary) {
        Class<T> cls = getClassForProxy(object);
        DomainTypeHandler<T> result = getDomainTypeHandler(cls, dictionary);
        return result;
    }

    @SuppressWarnings("unchecked")
    /** Get the domain class of the given proxy object.
     * @param object the object
     * @return the Domain class of the object
     */
    protected <T> Class<T> getClassForProxy(T object) {
        Class<?> cls = object.getClass();
        if (java.lang.reflect.Proxy.isProxyClass(cls)) {
            cls = proxyClassToDomainClass.get(cls);
        }
        return (Class<T>)cls;
    }

    public <T> T newInstance(Class<T> cls, Dictionary dictionary, Db db) {
        DomainTypeHandler<T> domainTypeHandler = getDomainTypeHandler(cls, dictionary);
        return domainTypeHandler.newInstance(db);
    }

    public Table getTable(String tableName, Dictionary dictionary) {
        Table result;
        try {
            result = dictionary.getTable(tableName);
        } catch(Exception ex) {
            throw new ClusterJFatalInternalException(
                        local.message("ERR_Get_Table"), ex);
        }
        return result;
    }

    public synchronized void close() {
        // close all of the cluster connections
        for (PooledConnection connection: pooledConnections) {
            connection.close();
        }
        pooledConnections.clear();
        synchronized(sessionFactoryMap) {
            // now remove this from the map
            sessionFactoryMap.remove(key);
        }
        state = State.Closed;
        GlobalConnectionPool.closeSessionFactory(spec.CONNECT_STRING, this);
    }

    public void setDomainTypeHandlerFactory(DomainTypeHandlerFactory domainTypeHandlerFactory) {
        this.domainTypeHandlerFactory = domainTypeHandlerFactory;
    }

    public DomainTypeHandlerFactory getDomainTypeHandlerFactory() {
        return domainTypeHandlerFactory;
    }

    public List<Integer> getConnectionPoolSessionCounts() {
        List<Integer> result = new ArrayList<Integer>();
        for (PooledConnection connection: pooledConnections) {
            result.add(connection.dbCount());
        }
        return result;
    }

    public String unloadSchema(Class<?> cls, Dictionary dictionary) {
        synchronized(typeToHandlerMap) {
            String tableName = null;
            DomainTypeHandler<?> domainTypeHandler = typeToHandlerMap.remove(cls);
            if (domainTypeHandler != null) {
                // remove the ndb dictionary cached table definition
                tableName = domainTypeHandler.getTableName();
                if (tableName != null) {
                    if (logger.isDebugEnabled())logger.debug("Removing dictionary entry for table " + tableName
                            + " for class " + cls.getName());
                    dictionary.removeCachedTable(tableName);
                    for (PooledConnection connection: pooledConnections) {
                        connection.unloadSchema(tableName);
                    }
                }
            }
            return tableName;
        }
    }

    /** Shut down the session factory by closing all pooled cluster connections
     * and restarting.
     * @since 7.5.7
     * @param cjde the exception that initiated the reconnection
     */
    public void checkConnection(ClusterJDatastoreException cjde) {
        if (CLUSTER_RECONNECT_TIMEOUT == 0) {
            return;
        } else {
            reconnect(CLUSTER_RECONNECT_TIMEOUT);
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /** Get the current state of this session factory.
     * @since 7.5.7
     * @see SessionFactory.State
     */
    public State currentState() {
        return state;
    }

    /** Reconnect this session factory using the default timeout value.
     * @since 7.5.7
     */
    public void reconnect() {
        reconnect(CLUSTER_RECONNECT_TIMEOUT);
    }

    /** Reconnect this session factory using the specified timeout value.
     * @since 7.5.7
     */
    public void reconnect(int timeout) {
        synchronized(this) {
            // if already restarting, do nothing
            if (State.Reconnecting.equals(state)) {
                logger.warn(local.message("WARN_Reconnect_already"));
                return;
            }
            // set the reconnect timeout to the current value
            CLUSTER_RECONNECT_TIMEOUT = timeout;
            if (timeout == 0) {
                logger.warn(local.message("WARN_Reconnect_timeout0"));
                return;
            }
            logger.warn(local.message("WARN_Reconnect", getConnectionPoolSessionCounts().toString()));
            pooledConnections.get(0).reconnect(timeout);
        }
    }

    private static int countSessions(List<Integer> sessionCounts) {
        int result = 0;
        for (int i: sessionCounts) {
            result += i;
        }
        return result;
    }

    int disconnect_check_active_sessions() {
        return countSessions(getConnectionPoolSessionCounts());
    }

    synchronized boolean check_pool_for_disconnect() {
        /* Check whether our own connections will be shutdown */
        if(pooledConnections.get(0).currentState().equals(State.Reconnecting)) {
            state = State.Reconnecting;
            return true;
        }
        return false;
    }

    void disconnect_set_closing() {
        List<Integer> sessionCounts = getConnectionPoolSessionCounts();
        if (countSessions(sessionCounts) != 0)
            logger.warn(local.message("WARN_Reconnect_timeout", sessionCounts.toString()));

        logger.warn(local.message("WARN_Reconnect_closing"));
        for (PooledConnection connection: pooledConnections) {
            connection.setClosing();
        }
    }

    void disconnect_close() {
       for (PooledConnection connection: pooledConnections) {
            connection.close();
        }
    }

    void do_reconnect() {
        pooledConnections.clear();
        // remove all DomainTypeHandlers, as they embed references to
        // Ndb dictionary objects which have been removed
        typeToHandlerMap.clear();

        logger.warn(local.message("WARN_Reconnect_creating"));
        createClusterConnectionPool();
        verifyConnectionPool();
        logger.warn(local.message("WARN_Reconnect_reopening"));
        synchronized(this) {
            state = State.Open;
        }
    }

    public void setRecvThreadCPUids(short[] cpuids) {
        // validate the size of the node ids with the connection pool size
        if (connectionPoolSize != cpuids.length) {
            throw new ClusterJUserException(
                    local.message("ERR_CPU_Ids_Must_Match_Connection_Pool_Size",
                            Arrays.toString(cpuids), connectionPoolSize));
        }
        // set cpuid to individual connections in the pool
        short newRecvThreadCPUids[] = new short[cpuids.length];
        try {
            int i = 0;
            for (PooledConnection connection: pooledConnections) {
                // No need to bind if the thread is already bound to same cpuid.
                if (cpuids[i] != recvThreadCPUids[i]){
                    if (cpuids[i] != -1) {
                        connection.setRecvThreadCPUid(cpuids[i]);
                    } else {
                        // cpu id is -1 which is a request for unlocking the thread from cpu
                        connection.unsetRecvThreadCPUid();
                    }
                }
                newRecvThreadCPUids[i] = cpuids[i];
                i++;
            }
            // binding success
            recvThreadCPUids = newRecvThreadCPUids;
        } catch (Exception ex) {
            // Binding cpuid failed.
            // To avoid partial settings, restore back the cpu bindings to the old values.
            for (int i = 0; newRecvThreadCPUids[i] != 0 && i < newRecvThreadCPUids.length; i++) {
                PooledConnection connection = pooledConnections.get(i);
                if (recvThreadCPUids[i] != newRecvThreadCPUids[i]) {
                    if (recvThreadCPUids[i] == -1) {
                        connection.unsetRecvThreadCPUid();
                    } else {
                        connection.setRecvThreadCPUid(recvThreadCPUids[i]);
                    }
                }
            }
            throw ex;
        }
    }

    public short[] getRecvThreadCPUids() {
        return recvThreadCPUids;
    }

    public void setRecvThreadActivationThreshold(int threshold) {
        if (threshold < 0) {
            // threshold should be a non negative value
            throw new ClusterJUserException(
                    local.message("ERR_Invalid_Activation_Threshold", threshold));
        }
        // any threshold above 15 is interpreted as 256 internally
        CLUSTER_RECV_THREAD_ACTIVATION_THRESHOLD = (threshold >= 16)?256:threshold;
        for (PooledConnection connection: pooledConnections) {
            connection.setRecvThreadActivationThreshold(threshold);
        }
    }

    public int getRecvThreadActivationThreshold() {
        return CLUSTER_RECV_THREAD_ACTIVATION_THRESHOLD;
    }
}
