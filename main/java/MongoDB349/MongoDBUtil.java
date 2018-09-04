package MongoDB349;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.*;

public abstract class MongoDBUtil implements MongoDAO{



    public boolean insert(MongoDatabase db, String table, Document doc) {
       // TODO Auto-generated method stub
        return false;
    }

    public boolean delete(MongoDatabase db, String table, BasicDBObject doc) {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean update(MongoDatabase db, String table, BasicDBObject oldDoc, BasicDBObject newDoc) {
        // TODO Auto-generated method stub
        return false;
    }
}
