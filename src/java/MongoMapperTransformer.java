import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.DataImporter;
import org.apache.solr.handler.dataimport.Transformer;

import java.util.Map;

/**
 * Created by danielgaffey on 1/2/17.
 */
public class MongoMapperTransformer extends Transformer
{
    private static final String MONGO_FIELD = "mongoField";

    @Override
    public Object transformRow(Map<String, Object> row, Context context)
    {
        for (Map<String, String> field : context.getAllEntityFields()) {

            String mongoFieldName = field.get(MONGO_FIELD);
            if (mongoFieldName == null) {
                continue;
            }

            String columnFieldName = field.get(DataImporter.COLUMN);
            row.put(columnFieldName, row.get(mongoFieldName));
        }

        return row;
    }
}
