package MongoDB349;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoDatabase;
import com.mongodb.ServerAddress;

/**
 *	主要是连接MongoDB、连接到表、关闭连接操作
 */
public class MongoDBConnection {
	//库名
	static final String DBName = "bsbank";
	//连接地址
	static final String ServerAddress = "60.24.65.74";
	//连接端口
	static final int PORT = 27010;
	//用户名
	static final String UserName = "bsbank";
	//密码
	static final String Password = "bsbank";
	//组装权限对象
    final List<MongoCredential> credentialsLists = new ArrayList<MongoCredential>();
   //组装mongo服务端地址
    ServerAddress  address = null;
    
	public MongoDBConnection(){
	}
	
	//连接到服务
	public MongoClient getMongoClient(){
		MongoClient mongoClient = null;
		try {
		  MongoCredential credential = MongoCredential.createCredential(UserName, DBName, Password.toCharArray());
		  credentialsLists.add(credential);
		  address = new ServerAddress(ServerAddress,PORT);
          mongoClient = new MongoClient(address, credentialsLists); 
          System.out.println("Connect to mongodb successfully");
      } catch (Exception e) {
          System.err.println(e.getClass().getName() + ": " + e.getMessage());
      }
      return mongoClient;
	}
	
	//连接到MongoDB的数据库
	public MongoDatabase getMongoDataBase(MongoClient mongoClient){
		MongoDatabase mongoDataBase = null;
		try {  
            if (mongoClient != null) {  
                mongoDataBase = mongoClient.getDatabase(DBName);  
                System.out.println("Connect to DataBase successfully");
            } else {  
                throw new RuntimeException("MongoClient不能够为空");  
            }  
        } catch (Exception e) {  
            e.printStackTrace();  
        }
        return mongoDataBase;
	}
	
	//关闭连接
	public void closeMongoClient(MongoDatabase mongoDataBase,MongoClient mongoClient){
		if (mongoDataBase != null) {  
            mongoDataBase = null;  
        }  
        if (mongoClient != null) {  
            mongoClient.close();  
        }  
        System.out.println("CloseMongoClient successfully");  
	}
}
