package Test;

import java.util.ArrayList;
import java.util.List;

import TDH484Hbase.CreateHBaseConnection;
import TDH484Hbase.GetRow;
import org.apache.hadoop.conf.Configuration;


/**
 *TDH141测试HBase
 */
public class HBaseTest 
{
    public static void main( String[] args ){
    	Configuration conf = CreateHBaseConnection.createHadoopConfiguration();
		//测试建表
//    	List<String> cf = new ArrayList<String>();
//    	cf.add("cf1");
//    	cf.add("cf2");
//    	CreateTbale ct = new CreateTbale();
//    	ct.create("TDHTable3", cf);
    	//测试插入数据
//    	InsertData.insertData(CreateHBaseConnection.createHadoopConfiguration(), "TDHTable1", "row", "cf2", "c1", "v");
    	//测试查询数据
//    	GetRow.getOneRow("TDHTable1", "row1");
    	GetRow.getAllRow("TDHTable1");
    	//删除行
//    	DeleteRow.deleteOneRow(CreateHBaseConnection.createHadoopConfiguration(), "TDHTable1", "row1");
//    	List<String> rows = new ArrayList<String>();
//    	rows.add("row2");
//    	rows.add("row3");
//    	DeleteRow.deleteRows(conf, "TDHTable1", rows);
    	//删除表或列族
//    	DeleteTableOrColumnFamily.deleteColumnsFamily("TDHTable1", "cf1");
//    	DeleteTableOrColumnFamily.deletTable("TDHTable2");
    }
}
