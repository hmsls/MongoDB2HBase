package MongoDB349;

import java.util.*;

import com.mongodb.BasicDBList;
import org.apache.commons.lang.ObjectUtils;
import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import UTIL.JsonStrToMap;

public class MongoDBQueryData /*implements MongoDAO*/ extends MongoDBUtil{

	//创建单个文档的字段列表，包含了所有的嵌套字段，嵌套的字段放在了嵌套的list里
	public  List<List<Object>> fieldsList = new ArrayList<List<Object>>();
	//一个集合collection（表）中的文档的个数
	public long docNum ;

	/**
	 * 根据给定表名（collection）来得到这个表（集合collection）所有的字段
	 * @param md MongoDatabase
	 * @param table collection name 表名
	 * @param mode	模式，可以通过ID查、文档查、查询全部
	 * @param key	通过文档、ID查询时需要指定的字段key
	 * @param val	通过文档、ID查询时需要指定的字段key
	 * @param fieldValue  是否在获取字段的时候获取字段的值
	 */
	public void getCollectionFieldAndValue(MongoDatabase md,String table,String mode,String key,String val,Boolean fieldValue){
		if(mode.equals("QA")){
			List<Map<Object,Object>> docmaplist = queryAll(md,table);
			//这里加了区分每一个文档后，需要很大的内存，导致GC过长，不适合全量查询插入
			//改用这个方法queryAllIdField得到所有的_id字段，然后循环调用文档查询方法查询
			Iterator<Map<Object,Object>> docmapit = docmaplist.iterator();
			while (docmapit.hasNext()){
				Map<Object,Object> mapdoc = docmapit.next();
				getCollectionFieldRecursion(mapdoc,"",fieldValue);
			}
		}else if(mode.equals("BD")){
			BasicDBObject basicDoc = new BasicDBObject(key,val);
			List<Map<Object,Object>> maplist = queryByDoc(md,table,basicDoc);
			Iterator<Map<Object,Object>> maplistit = maplist.iterator();
			while(maplistit.hasNext()){
				Map<Object,Object> docmap = maplistit.next();
				getCollectionFieldRecursion(docmap,"",fieldValue);
			}
		}else if(mode.equals("BI")){
			Map<Object,Object> doc = queryByID(md,table,val);
			getCollectionFieldRecursion(doc,"",fieldValue);
		}
	}

	/**
	 * 通过递归的方法，把一个文档中的所有的字段都放到一个列表里，这个列表是嵌套的list，里面的list放各个子节点的字段
	 *	获得一个文档中的所有的字段，并且获得相同层级的字段的路径
	 * @param documentMap  递归的文档map
	 * @param fieldValue	是否要获取值
	 */
	public void getCollectionFieldRecursion(Map<Object,Object> documentMap,Object fieldPath,Boolean fieldValue){
		Set<Map.Entry<Object,Object>> docset = documentMap.entrySet();
		Iterator<Map.Entry<Object,Object>> docentry = docset.iterator();
		//创建字段列表
		List<Object> fields = new ArrayList<Object>();
		fields.add(fieldPath);
		while (docentry.hasNext()){
			Map.Entry<Object,Object> entry = docentry.next();
			Object key = entry.getKey();
			Object value = entry.getValue();
            /*这里只添加具体的string类型元素，对象和集合类型的元素不添加，因为集合的第一个字段是路径，所以如果一个集合只有
            一个值，那么这个集合就只有一个路径元素，剩余元素都为对象或者集合，因此这个集合就可以直接丢掉不用*/
			if(fieldValue){
				if(!value.getClass().isInstance(new BasicDBObject()) && !value.getClass().isInstance(new BasicDBList())){
					if(value.equals("") || value == null){
						value = "FieldWithoutValue";
						fields.add(key+"=="+value);
					}else {
						fields.add(key + "==" + value);
					}
				}
			}else{
				if(!value.getClass().isInstance(new BasicDBObject()) && !value.getClass().isInstance(new BasicDBList())){
					fields.add(key);
				}
			}
			if(value.getClass().isInstance(new BasicDBObject())){
				String subDoc = ((BasicDBObject) value).toJson();
				Map<Object,Object> subMap = JsonStrToMap.jsonStrToMap(subDoc);
				StringBuilder sb = new StringBuilder();
				sb = sb.append(fieldPath).append("/").append(key);
				getCollectionFieldRecursion(subMap,sb,fieldValue);
			}
			if(value.getClass().isInstance(new BasicDBList())){
				Map<Object,Object> listMap = ((BasicDBList)value).toMap();
				StringBuilder sb = new StringBuilder();
				sb = sb.append(fieldPath).append("/").append(key);
				getCollectionFieldRecursion(listMap,sb,fieldValue);
			}
		}
		fieldsList.add(fields);
	}

	//查询一条具体的记录
	public Map<Object, Object> queryByID(MongoDatabase db, String table, Object Id) {
		MongoCollection<Document> collection = db.getCollection(table);
		//DBObject接口和BasicDBObject对象：表示一个具体的记录，
		//BasicDBObject实现了DBObject，是key-value的数据结构，用起来和HashMap是基本一致的。
		BasicDBObject query = new BasicDBObject("_id",Id);
		FindIterable<Document> iterable = collection.find(query);
		Map<Object,Object> jsonStrToMap = null;
		MongoCursor<Document> cursor = iterable.iterator();
		while(cursor.hasNext()){
			Document doc = cursor.next();
			String jsonString1 = doc.toJson();
			jsonStrToMap = JsonStrToMap.jsonStrToMap(jsonString1);
		}
		System.out.println("检索ID完毕");
		return jsonStrToMap;
	}
	
	//根据文档来查询
	//测试根据文档来查询，可以通过指定某个字段及其值来查询某个文档，可以作为增量查询的方法。
	//可能很多文档都有相同个一个字段和相同的值，这样可以筛选出来。
	//可以得到一个文档的全部字段
	public List<Map<Object,Object>> queryByDoc(MongoDatabase db,String table,BasicDBObject doc){
		MongoCollection<Document> collection =  db.getCollection(table);
		FindIterable<Document> iterable = collection.find(doc);
		List<Map<Object,Object>> list = new ArrayList<Map<Object,Object>>();
		MongoCursor<Document> cursor = iterable.iterator();
		while(cursor.hasNext()){
			Document document = cursor.next();
			String jsonString = document.toJson();
			Map<Object,Object> jsonStrToMap = JsonStrToMap.jsonStrToMap(jsonString);
			list.add(jsonStrToMap);
		}
		System.out.println("检索doc完毕");
        return list;
	}
	
	//查询全部文档
	public List<Map<Object,Object>> queryAll(MongoDatabase db,String table){
		MongoCollection<Document> collection = db.getCollection(table);
		docNum = collection.count();
		FindIterable<Document> iterable = collection.find();
		List<Map<Object,Object>> list = new ArrayList<Map<Object,Object>>();
		MongoCursor<Document> cursor = iterable.iterator();
		while(cursor.hasNext()){
			Document docs = cursor.next();
			String jsonString = docs.toJson();
			Map<Object,Object> jsonStrToMap = JsonStrToMap.jsonStrToMap(jsonString);
			list.add(jsonStrToMap);
		}
		System.out.println("检索全部文档完毕");
        return list;
	}

	//查询全部文档的_id字段，然后通过这个字段和其值来通过循环执行bydoc方法查找每个文档
	public List<String> queryAllIdField(MongoDatabase db,String table){
		MongoCollection<Document> collection = db.getCollection(table);
		FindIterable<Document> iterable = collection.find();
		MongoCursor<Document> cursor = iterable.iterator();
		List<String> collIdFieldsList = new ArrayList<String>();
		while(cursor.hasNext()){
			Document docs = cursor.next();
			String jsonString = docs.toJson();
			Map<Object,Object> jsonStrToMap = JsonStrToMap.jsonStrToMap(jsonString);
			String idValue = (String)jsonStrToMap.get("_id");
			collIdFieldsList.add(idValue);
		}
		System.out.println("检索全部文档的_id字段完毕");
		return collIdFieldsList;
	}

}
