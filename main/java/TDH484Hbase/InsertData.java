package TDH484Hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class InsertData {
	/**
	 *列是动态增加的，就是添加值的时候，直接在某一个列族下指定列名，这样就创建了列，同时将值插入
	 *参数qualifier就是列名，根据行、列族、列名指定唯一的一个数据cell单元格
	 */
	static Configuration conf = CreateHBaseConnection.createHadoopConfiguration();
	public static void insertData(String tableName,String rowName,String columnsFamily,List<String> columns,List<String> values){
		try {
			HTable htable = new HTable(conf, Bytes.toBytes(tableName));
			List<Put> puts = new ArrayList<Put>();
			int count = 0;
			//批量添加值
			for(int i = 0;i<columns.size();i++){
				Put put = new Put(Bytes.toBytes(rowName));
				put.add(Bytes.toBytes(columnsFamily), Bytes.toBytes(columns.get(i)), Bytes.toBytes(values.get(i)));
				puts.add(put);
				count += 1;
			}
			htable.put(puts);
			System.out.println(tableName + "插入" + count + "条数据成功！");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
