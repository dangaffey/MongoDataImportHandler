import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import org.apache.solr.handler.IndexFetcher;
import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.DataImportHandlerException;
import org.apache.solr.handler.dataimport.DataSource;
import org.apache.solr.handler.dataimport.TemplateTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;

/**
 * Created by danielgaffey on 12/18/16.
 */
public class MongoDataSource extends DataSource<Iterator<Map<String, Object>>>
{
    private static final Logger LOG = LoggerFactory.getLogger(TemplateTransformer.class);
    private MongoDatabase mongoDB;


    /**
     * Basic initialization method to setup the MongoClient
     *
     * @param context
     * @param initProps
     */
    public void init(Context context, Properties initProps)
    {
        MongoClientBuilder builder = new MongoClientBuilder(initProps);
        MongoClient client = builder.build();

        mongoDB = client.getDatabase(builder.getDatabaseName());
        if (mongoDB == null) {
            throw new DataImportHandlerException(SEVERE, "Unable to connect to database");
        }
    }


    public Iterator<Map<String, Object>> getData(String query)
    {
        return null;
    }

    public void close()
    {

    }


}
