package org.apache.solr.handler.dataimport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.solr.common.params.CollectionAdminParams.COLLECTION;
import static org.apache.solr.common.params.CoreAdminParams.NAME;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;
import static org.apache.solr.handler.dataimport.SolrEntityProcessor.QUERY;
import static org.apache.solr.handler.dataimport.SqlEntityProcessor.*;

/**
 * Created by danielgaffey on 1/2/17.
 */
public class MongoEntityProcessor extends EntityProcessorBase
{
    private static final Logger LOG = LoggerFactory.getLogger(EntityProcessorBase.class);
    private static final String FULL = "FULL_DUMP";
    private static final String DELTA = "DELTA_DUMP";
    private static final String DATASOURCE = "datasource";

    private MongoDataSource dataSource;
    private String collectionName;


    public void init(Context context)
    {
        super.init(context);
        collectionName = context.getEntityAttribute(COLLECTION);

        if (collectionName == null) {
            throw new DataImportHandlerException(SEVERE, "Collection name must be supplied");
        }

        dataSource = (MongoDataSource) context.getDataSource(context.getEntityAttribute(DATASOURCE));
    }




    private void initQuery(String queryString)
    {
        try {
            //Can't be accessed outside of package, is this necessary?
            //DataImporter.QUERY_COUNT.get().incrementAndGet();
            rowIterator = dataSource.getData(queryString, collectionName);
            query = queryString;
        } catch (DataImportHandlerException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("The query failed '" + queryString + "'", e);
            throw new DataImportHandlerException(SEVERE, e);
        }
    }


    /**
     * Provides the next row result from the query
     *
     * @return
     */
    public Map<String, Object> nextRow()
    {
        if (rowIterator == null) {
            initQuery(context.replaceTokens(getQuery()));
        }

        return getNext();
    }


    /**
     * Provides the next modified row for a delta import query
     *
     * @return
     */
    public Map<String, Object> nextModifiedRowKey()
    {
        if (rowIterator == null) {
            String query = context.getEntityAttribute(DELTA_IMPORT_QUERY);
            if (query == null) {
                return null;
            }

            initQuery(context.replaceTokens(query));
        }

        return getNext();
    }




    public Map<String, Object> nextDeletedRowKey()
    {
        if (rowIterator == null) {
            String deletedPkQuery = context.getEntityAttribute(DEL_PK_QUERY);
            if (deletedPkQuery == null) {
                return null;
            }

            initQuery(context.replaceTokens(deletedPkQuery));
        }

        return getNext();
    }



    public Map<String, Object> nextModifiedParentRowKey()
    {
        if (rowIterator == null) {
            String parentDeltaQuery = context.getEntityAttribute(PARENT_DELTA_QUERY);
            if (parentDeltaQuery == null) {
                return null;
            }

            LOG.info("Running parentDeltaQuery for Entity: " + context.getEntityAttribute(NAME));
            initQuery(context.replaceTokens(parentDeltaQuery));
        }

        return getNext();
    }


    /**
     * Attempts to provide the correct query based on the active process
     *
     * @return
     */
    private String getQuery()
    {
        String queryString = context.getEntityAttribute(QUERY);

        if (context.currentProcess().equals(FULL)) {
            return queryString;
        }

        if (context.currentProcess().equals(DELTA)) {
            return context.getEntityAttribute(DELTA_IMPORT_QUERY);
        }

        return null;
    }
}
