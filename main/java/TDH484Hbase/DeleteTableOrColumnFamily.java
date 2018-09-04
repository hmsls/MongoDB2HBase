package TDH484Hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;


/**
 *	删除表，或者删除列族
 */
public class DeleteTableOrColumnFamily {
	
//	public static CreateHBaseConnection conf = new CreateHBaseConnection();
	public static Configuration conf = CreateHBaseConnection.createHadoopConfiguration();
	public static CreateTbale ct = new CreateTbale();
	
	//删除表，首先要下线表，然后删除表
	public static void deletTable(String tableName){
		try {
			HBaseAdmin hadmin = new HBaseAdmin(conf);
			if(ct.isTableExists(tableName)){
				hadmin.disableTable(Bytes.toBytes(tableName));
				hadmin.deleteTable(Bytes.toBytes(tableName));
				System.out.println("删除表"+tableName+"成功！");
			}else{
				System.out.println("表" + tableName + " 不存在!");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	//删除列族，即使不disable表，也可以删除列族
	public static void deleteColumnsFamily(String tableName,String columnsFamily){
		HBaseAdmin hadmin;
		try {
			hadmin = new HBaseAdmin(conf);
			if(ct.isTableExists(tableName)){
				hadmin.deleteColumn(tableName, columnsFamily);
				System.out.println(tableName+"删除列族"+columnsFamily+"成功！");
			}else{
				System.out.println("表" + tableName + " 不存在！无法删除列族"+columnsFamily);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
