import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import org.apache.solr.handler.dataimport.DataImportHandlerException;

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

    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private static final String DATABASE = "database";
    private static final String READ_FROM_SECONDARY = "readFromSecondary";


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

        String host = initProps.getProperty(HOST);
        if (host != null) {
            setHost(host);
        }

        String port = initProps.getProperty(PORT);
        if (port != null) {
            setPort(port);
        }

        String username = initProps.getProperty(USERNAME);
        String password = initProps.getProperty(PASSWORD);
        if (username != null && password != null) {
            createMongoCredential(username, databaseName, password);
        }
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
        creds.add(MongoCredential.createCredential(user, db, pass.toCharArray()));
    }


    /**
     * Provides the database name
     */
    public String getDatabaseName()
    {
        return databaseName;
    }


    /**
     *
     * @return
     */
    public MongoClient build()
    {
        return null;
    }

}
