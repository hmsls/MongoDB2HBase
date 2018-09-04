package TDH484Hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *	删除一行或者批量删除行
 */
public class DeleteRow {
	//删除一行
	public static void deleteOneRow(Configuration conf,String tableName,String rowName){
		try {
			HTable htable = new HTable(conf, Bytes.toBytes(tableName));
			Delete del = new Delete(Bytes.toBytes(rowName));
			htable.delete(del);
			System.out.println("删除行"+rowName+"成功！");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	//删除多行
	public static void deleteRows(Configuration conf,String tableName,List<String> rows){
		try {
			int count = 0;
			HTable htable = new HTable(conf, Bytes.toBytes(tableName));
			for(String row : rows){
				Delete del = new Delete(Bytes.toBytes(row));
				htable.delete(del);
				count += 1;
			}
			System.out.println("批量删除"+count+"行成功！");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
