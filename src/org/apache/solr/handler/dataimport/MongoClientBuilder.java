package org.apache.solr.handler.dataimport;

import com.mongodb.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;

/**
 * Created by danielgaffey on 12/18/16.
 */
public class MongoClientBuilder
{
    private List<String> hosts = new ArrayList<>();
    private List<String> ports = new ArrayList<>();
    private List<MongoCredential> creds = new ArrayList<>();
    private String databaseName;
    private ReadPreference readPreference;

    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private static final String DATABASE = "database";
    private static final String PRIMARY = "primary";
    private static final String SECONDARY = "secondary";
    private static final String READ_PREFERENCE ="readPreference";


    /**
     * Provides parsing of the init properties
     *
     * @param initProps
     */
    public MongoClientBuilder(Properties initProps)
    {
        databaseName = initProps.getProperty(DATABASE);
        if (databaseName == null) {
            throw new DataImportHandlerException(SEVERE, "Database must be supplied");
        }

        String readPreference = initProps.getProperty(READ_PREFERENCE);
        if (readPreference == null) {
            throw new DataImportHandlerException(SEVERE, "Datasource must be named");
        }
        setReadPreference(readPreference);


        String host = initProps.getProperty(HOST);
        if (host == null) {
            throw new DataImportHandlerException(SEVERE, "Host(s) must be supplied");
        }
        setHost(host);

        String port = initProps.getProperty(PORT);
        if (port == null) {
            throw new DataImportHandlerException(SEVERE, "Port(s) must be supplied");
        }
        setPort(port);


        if (hosts.size() != ports.size()) {
            throw new DataImportHandlerException(SEVERE, "A port must be supplied foreach host");
        }


        String username = initProps.getProperty(USERNAME);
        String password = initProps.getProperty(PASSWORD);
        if (username != null && password != null) {
            createMongoCredential(username, databaseName, password);
        }
    }


    /**
     * Attempts to set the preferred read preference for MongoDB
     *
     * @param readPreference
     */
    private void setReadPreference(String readPreference)
    {
        if (readPreference.equals(PRIMARY)) {
            this.readPreference = ReadPreference.primaryPreferred();
            return;
        }

        if (readPreference.equals(SECONDARY)) {
            this.readPreference = ReadPreference.secondaryPreferred();
            return;
        }

        throw new DataImportHandlerException(SEVERE, "Read preference options are 'primary' or 'secondary'");
    }


    /**
     * Attempts to parse multiple hosts for a replica set
     *
     * @param host
     */
    private void setHost(String host)
    {
        if (host.contains(",")) {
            hosts = Arrays.asList(host.split(","));
            return;
        }

        hosts.add(host);
    }


    /**
     * Attempts to parse multiple ports for a replica set
     *
     * @param port
     */
    private void setPort(String port)
    {
        if (hosts.contains(",")) {
            ports = Arrays.asList(port.split(","));
            return;
        }

        ports.add(port);
    }


    /**
     * Attempts to create a MongoCredential for authentication with MongoDB
     *
     * @param user
     * @param pass
     * @param db
     */
    private void createMongoCredential(String user, String db, String pass)
    {
        creds.add(MongoCredential.createScramSha1Credential(user, db, pass.toCharArray()));
    }


    /**
     * Provides a client that is intended to connect to a standalone node
     *
     * @return
     */
    private MongoClient createSingleInstanceClient()
    {
        return new MongoClient(
                new ServerAddress(hosts.get(0), Integer.parseInt(ports.get(0))),
                creds,
                MongoClientOptions
                        .builder()
                        .readPreference(readPreference)
                        .build()
        );
    }


    /**
     * Provides a client that is intended to connect to a replica set
     *
     * @return
     */
    private MongoClient createReplicaSetClient()
    {
        List<ServerAddress> addresses = new ArrayList<>();
        for (int i = 0; i < hosts.size(); i++) {
            addresses.add(new ServerAddress(hosts.get(i), Integer.parseInt(ports.get(i))));
        }

        return new MongoClient(
                addresses,
                creds,
                MongoClientOptions
                        .builder()
                        .readPreference(readPreference)
                        .build()
        );
    }


    /**
     * Provides the database name
     */
    public String getDatabaseName()
    {
        return databaseName;
    }


    /**
     * Builds the MongoClient based on the provided initialization properties
     *
     * @return
     */
    public MongoClient build()
    {
        if (hosts.size() == 1) {
            return createSingleInstanceClient();
        }

        return createReplicaSetClient();
    }


}
