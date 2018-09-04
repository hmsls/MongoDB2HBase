package Test;

import java.lang.reflect.Type;
import java.util.*;
import java.util.Map.Entry;

import UTIL.JsonStrToMap;
import com.google.common.collect.Iterators;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.client.*;

import MongoDB349.MongoDBConnection;
import MongoDB349.MongoDBQueryData;
import com.sun.xml.internal.ws.util.QNameMap;
import org.bson.Document;

public class MongoDBTest {
    //创建文档字段列表，包含了所有的子文档字段，子文档字段放在了嵌套的list里
    public static List<List<Object>> fieldsList = new ArrayList<List<Object>>();
    public static Map<Object,List<Object>> fieldsMap = new HashMap<Object, List<Object>>();

	public static void main(String[] args) {
		try {
			MongoDBQueryData mq = new MongoDBQueryData();
			MongoDBConnection mc = new MongoDBConnection();
			MongoDatabase md = mc.getMongoDataBase(mc.getMongoClient());
//			testQueryAllMethod(mq,md);

			List<String> idlist = queryAll(md,"other_petn3002_test");
			for(String id:idlist){
                System.out.println(id);
            }

//            getCollectionField(mq,md,"other_petn3002_test");
//            System.out.println(fieldsList.size());
//            System.out.println(fieldsList.get(0));
//            for(int i=0;i<fieldsList.size();i++){
//                System.out.println(fieldsList.get(i));
//            }
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	//测试根据文档来查询，可以通过指定某个字段及其值来查询某个文档，可以作为增量查询的方法。
    //可能很多文档都有相同个一个字段和相同的值，这样可以筛选出来。
    //可以得到一个文档的全部字段
    public static void testQueryByDoc(MongoDBQueryData mq,MongoDatabase md){
	    BasicDBObject basicdbdoc = new BasicDBObject("_id","7816014d-e594-40d1-920e-624394b99b69");
	    List<Map<Object,Object>> doclist = mq.queryByDoc(md,"manual_review_log",basicdbdoc);
	    Iterator<Map<Object,Object>> docs = doclist.iterator();
	    while(docs.hasNext()){
	        Map<Object,Object> doc = docs.next();
	        Set<Entry<Object,Object>> entrys = doc.entrySet();
	        Iterator<Entry<Object,Object>> en = entrys.iterator();
	        while(en.hasNext()){
	            Entry e = en.next();
	            Object key = e.getKey();
	            Object value = e.getValue();
                System.out.println("fakey :"+key+"-----favalue :"+value.getClass());
            }
        }
    }


	//测试根据一条记录的_id来查询，可以得到一个文档的全部字段
	public static void testQueryByID(MongoDBQueryData mq,MongoDatabase md){
        Map<Object,Object> res = null;
        try {
            res = mq.queryByID(md,"manual_review_log", "7816014d-e594-40d1-920e-624394b99b69");
        } catch (Exception e) {
            e.printStackTrace();
        }
        Set<Entry<Object,Object>> set = res.entrySet();
        Iterator it = set.iterator();
        while(it.hasNext()){
            Entry e = (Entry) it.next();
            Object key = e.getKey();
            Object value =  e.getValue();
            System.out.println("key :"+key+"-----value :"+value);
        }
    }
    //测试查询文档的全部，包括嵌套的的json格式内容，目前只支持一层嵌套，可以得到全部文档的字段
    //可以写一个递归查询，得到文档的全部字段
	public static void testQueryAllMethod(MongoDBQueryData mq,MongoDatabase md){
        List<Map<Object,Object>> res = mq.queryAll(md,"manual_review_log");
        //获得文档个数
        System.out.println(res.size());
        Iterator<Map<Object,Object>> it = res.iterator();
        while(it.hasNext()){
            Map<Object,Object> map = it.next();
            Set<Object> keys = map.keySet();
            Iterator keyIt = keys.iterator();
            while(keyIt.hasNext()){
                Object key = keyIt.next();
                Object value = map.get(key);
                if(((Class) value.getClass()).isInstance(new BasicDBObject())){
//                    System.out.println("key :"+key+"-----value :"+value);
                    BasicDBObject bo = (BasicDBObject)value;
                    String subJson = bo.toJson();
                    Map<Object,Object> ma = JsonStrToMap.jsonStrToMap(subJson);
                    Set<Object> ks =  ma.keySet();
                    Iterator<Object> ksit = ks.iterator();
                    while(ksit.hasNext()){
                        Object k = ksit.next();
                        Object v = ma.get(k);
//                        System.out.println("~~~~~~~~~~~subkey :"+k+"-----subvalue :"+v);
                    }
                }
//                System.out.println("fakey :"+key+"-----favalue :"+value);
            }
        }
    }

    //测试得到文档的所有的字段
    public static void getCollectionField(MongoDBQueryData mq,MongoDatabase md,String table){
	    BasicDBObject basicdbobj = new BasicDBObject("_id","6204c4aa-6a48-4d33-a308-4562465fea16");
	    List<Map<Object,Object>> docmaplist = mq.queryByDoc(md,table,basicdbobj);
	    Iterator<Map<Object,Object>> docmapit = docmaplist.iterator();
	    while (docmapit.hasNext()){
	        Map<Object,Object> mapdoc = docmapit.next();
	        getCollectionFieldRecursion("/",mapdoc);
        }
    }
    //获得一个文档中的所有的字段，并且获得相同层级的字段的路径
    public static void getCollectionFieldRecursion(Object fatherValue,Map<Object,Object> documentMap){
        Set<Entry<Object,Object>> docset = documentMap.entrySet();
        Iterator<Entry<Object,Object>> docentry = docset.iterator();
        //创建字段列表
        List<Object> fields = new ArrayList<Object>();
        System.out.println("fatherValue :"+fatherValue);
        fields.add(fatherValue);
        while (docentry.hasNext()){
            Entry<Object,Object> entry = docentry.next();
            Object key = entry.getKey();
            Object value = entry.getValue();
            /*这里只添加具体的string类型元素，对象和集合类型的元素不添加，因为集合的第一个字段是路径，所以如果一个集合只有
            一个值，那么这个集合就只有一个路径元素，剩余元素都为对象或者集合，因此这个集合就可以直接丢掉不用*/
            if(!value.getClass().isInstance(new BasicDBObject()) && !value.getClass().isInstance(new BasicDBList())){
                fields.add( key/*+"="+value*/);
            }
            System.out.println("key :"+key+/*"-----value :"+ value + */"-----value type" + value.getClass());
            if(value.getClass().isInstance(new BasicDBObject())){
                String subDoc = ((BasicDBObject) value).toJson();
                Map<Object,Object> subMap = JsonStrToMap.jsonStrToMap(subDoc);
                StringBuilder sb = new StringBuilder();
                sb = sb.append(fatherValue).append("/").append(key);
                getCollectionFieldRecursion(sb,subMap);
            }
            if(value.getClass().isInstance(new BasicDBList())){
                Map<Object,Object> listMap = ((BasicDBList)value).toMap();
                StringBuilder sb = new StringBuilder();
                sb = sb.append(fatherValue).append("/").append(key);
                getCollectionFieldRecursion(sb,listMap);
            }
        }
        fieldsList.add(fields);
    }

    public static List<Object> getFields(Object fatherValue,Map<Object,Object> documentMap){
	    Set<Entry<Object,Object>> docset = documentMap.entrySet();
	    Iterator<Entry<Object,Object>> docit = docset.iterator();
	    List<Object> fieldsList = new ArrayList<Object>();
	    fieldsList.add(fatherValue);
	    while (docit.hasNext()){
	        Entry<Object,Object> entry = docit.next();
	        Object keyobj = entry.getKey();
	        Object valobj = entry.getValue();
	        fieldsList.add(keyobj);
        }
        return fieldsList;
    }

    //使用json解析
    public static void testJsonPar(MongoDatabase md,String table, Object Id){
	    String jsonDoc = null;
	    MongoCollection<Document> coll = md.getCollection(table);
	    BasicDBObject bo = new BasicDBObject("_id",Id);
	    FindIterable<Document> docit = coll.find(bo);
	    MongoCursor<Document> it = docit.iterator();
	    while (it.hasNext()){
	        Document doc = it.next();
	        jsonDoc = doc.toJson();
//            System.out.println(jsonDoc);
        }
        JsonParser jp = new JsonParser();
	    JsonElement je = jp.parse(jsonDoc);
	    if(je.isJsonArray()){
	        JsonArray ja = je.getAsJsonArray();
	        for(int i=0;i<ja.size();i++){
                System.out.println(ja.get(i).toString());
            }
        }
        if(je.isJsonObject()){
            JsonObject jo = je.getAsJsonObject();
            Set<String> key = jo.keySet();

            Iterator<String> joit = key.iterator();
            while (joit.hasNext()){
                String joKey = joit.next();
                JsonElement joele = jo.get(joKey);
                String v = joele.toString();
                System.out.println(joKey+"---"+v);
            }
        }

    }

    //直接读取指定路径上的是字段
    public static void testReadPathFields(MongoDatabase md,String table,BasicDBObject bo){
	    MongoCollection<Document> coll = md.getCollection(table);
	    FindIterable<Document> findit = coll.find(new BasicDBObject("_id","6204c4aa-6a48-4d33-a308-4562465fea16"));
	    MongoCursor<Document> cursor = findit.iterator();
	    while(cursor.hasNext()){
	        Document doc = cursor.next();
	        String val = doc.getString("$eleMatch:msgname");
            System.out.println(val);
        }

    }

    //查询全部文档的_id字段，然后通过这个字段和其值来通过循环执行bydoc方法查找每个文档
    public static List<String> queryAll(MongoDatabase db,String table){
        MongoCollection<Document> collection = db.getCollection(table);
        FindIterable<Document> iterable = collection.find();
        List<String> list = new ArrayList<String>();
        MongoCursor<Document> cursor = iterable.iterator();
        while(cursor.hasNext()){
            Document docs = cursor.next();
            String jsonString = docs.toJson();
            Map<Object,Object> jsonStrToMap = JsonStrToMap.jsonStrToMap(jsonString);
            String idValue = (String)jsonStrToMap.get("_id");
            list.add(idValue);
        }
        System.out.println("检索全部文档的_id字段完毕");
        return list;
    }
}
