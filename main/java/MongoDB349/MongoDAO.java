package MongoDB349;

import java.util.Map;
import org.bson.Document;
import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoDatabase;
/**
 *	增删改查的接口
 */
public interface MongoDAO {
	/**
     * Get Data BY ID
     * 
     * @param db
     * @param table
     * @param Id
     * @throws Exception 
     */
    public Map<Object,Object> queryByID(MongoDatabase db, String table, Object Id) throws Exception;

    /**
     * Insert Data
     */
    public boolean insert(MongoDatabase db, String table, Document doc);

    /**
     * Delete Many Data.if doc is empty will delete all Data
     */
    public boolean delete(MongoDatabase db, String table, BasicDBObject doc);

    /**
     * Update All Data
     */
    public boolean update(MongoDatabase db, String table, BasicDBObject oldDoc, BasicDBObject newDoc);
}
