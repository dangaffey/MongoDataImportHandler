package org.apache.solr.handler.dataimport;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;

/**
 * Created by danielgaffey on 12/18/16.
 */
public class MongoDataSource extends DataSource<Iterator<Map<String, Object>>>
{
    private static final Logger LOG = LoggerFactory.getLogger(TemplateTransformer.class);

    private MongoClient client;
    private MongoDatabase mongoDB;
    private MongoCollection<Document> collection;
    private MongoCursor cursor;


    /**
     * Basic initialization method to setup the MongoClient
     *
     * @param context
     * @param initProps
     */
    public void init(Context context, Properties initProps)
    {
        MongoClientBuilder builder = new MongoClientBuilder(initProps);
        client = builder.build();

        mongoDB = client.getDatabase(builder.getDatabaseName());
        if (mongoDB == null) {
            throw new DataImportHandlerException(SEVERE, "Unable to connect to database");
        }
    }


    public Iterator<Map<String, Object>> getData(String query)
    {
        BasicDBObject queryObject = (BasicDBObject) JSON.parse(query);
        LOG.debug("Executing Mongo Query: " + query);

        long start = System.currentTimeMillis();

        cursor = collection.find(queryObject).iterator();
        LOG.trace("Execution Time for Query: " + (System.currentTimeMillis() - start));

        return new ResultSetIterator(cursor);
    }



    public Iterator<Map<String, Object>> getData(String query, String collectionName)
    {
        collection = mongoDB.getCollection(collectionName);
        return getData(query);
    }


    /**
     * Provides the closing interface to disconnect from the Mongo Instance(s)
     */
    public void close()
    {
        if (cursor != null) {
            cursor.close();
        }

        if (client != null) {
            client.close();
        }
    }


    /**
     * Provides a Solr-compliant iterator to iterate through the results of Mongo queries
     */
    private static class ResultSetIterator implements Iterator<Map<String, Object>>
    {
        private final MongoCursor cursor;

        ResultSetIterator(MongoCursor cursor)
        {
            this.cursor = cursor;
        }

        @Override
        public boolean hasNext()
        {
            return cursor.hasNext();
        }

        @Override
        public Map<String, Object> next()
        {
            Document document = (Document) cursor.next();

            Map<String, Object> map = new HashMap<>();
            Set<String> keys = document.keySet();
            for (String key : keys) {
                map.put(key, document.get(key));
            }

            return map;
        }
    }

}
