package Test;

import MongoDB349.MongoDAO;
import MongoDB349.MongoDBConnection;
import MongoDB349.MongoDBQueryData;
import MongoDB349.MongoDBUtil;
import TDH484Hbase.CreateTbale;
import TDH484Hbase.GetRow;
import TDH484Hbase.InsertData;
import UTIL.UpdateList;
import com.mongodb.client.MongoDatabase;

import java.util.ArrayList;
import java.util.List;

import static Test.MongoDBTest.fieldsList;

public class APP {

    static List<String> fields = new ArrayList<String>();
    static List<String> values = new ArrayList<String>();
    static MongoDBConnection mc = new MongoDBConnection();
    static MongoDatabase md = mc.getMongoDataBase(mc.getMongoClient());
    static MongoDBQueryData mq = new MongoDBQueryData();

    public static void main(String[] args){
        Long start = System.currentTimeMillis();
        testQueryAllColl(md,"other_petn3002");
        System.out.println("用时：" + (System.currentTimeMillis()-start) + "毫秒。");
        System.out.println();
    }

    public static void testQueryOneDoc(MongoDatabase md,String table){
        //获取某个表中的所有字段和值，然后通过成员变量fieldsList拿到所有字段，通过更新字段得到加了路径的字段，然后
        //将其插入HBase
        mq.getCollectionFieldAndValue(md,table,"BD","_id","6204c4aa-6a48-4d33-a308-4562465fea16",true);
        //得到某个文档的所有字段的列表
        List<List<Object>> res = mq.fieldsList;
        //某个文档把路径的代号加入到每个字段中
        List<List<Object>> res1 = UpdateList.updateListElement(res);
        //得到某个文档中的所有字段和字段的值
        for(int j=0;j<res1.size();j++){
            List<Object> coll = res1.get(j);
            for (int i = 1; i < coll.size(); i++) {
                Object resutl = coll.get(i);
                String[] fv = ((String)resutl).split("==");
                fields.add(fv[0]);
                values.add(fv[1]);
//                System.out.println(fv[0]);
//                System.out.println(fv[1]);
            }
        }
        InsertData.insertData("mongohbase","row2","cf1",fields,values);
    }

    public static void testQueryAllColl(MongoDatabase md,String table){
        List<String> idValList = mq.queryAllIdField(md,table);
        for(String idval:idValList){
            //获取某个表中的所有字段和值，然后通过成员变量fieldsList拿到所有字段，通过更新字段得到加了路径的字段，然后
            //将其插入HBase
            mq.getCollectionFieldAndValue(md,table,"BD","_id",idval,true);
            //得到某个文档的所有字段的列表
            List<List<Object>> res = mq.fieldsList;
            //某个文档把路径的代号加入到每个字段中
            List<List<Object>> res1 = UpdateList.updateListElement(res);
            //得到某个文档中的所有字段和字段的值
            for(int j=0;j<res1.size();j++){
                List<Object> coll = res1.get(j);
                for (int i = 1; i < coll.size(); i++) {
                    Object resutl = coll.get(i);
                    String[] fv = ((String)resutl).split("==");
                    fields.add(fv[0]);
                    values.add(fv[1]);
//                    System.out.println(fv[0]);
//                    System.out.println(fv[1]);
                }
            }
            //截取_id前6个字符
            String row = idval.substring(0,6);
            InsertData.insertData("mongohbase",row,"cf1",fields,values);
            //清除上一个文档的数据，这些已经插入到了HBase，所以要清除，否则会重复，然后报字段整合问题的错
            mq.fieldsList.clear();
        }
    }
}
