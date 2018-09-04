package TDH484Hbase;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 查询表中的数据
 */
public class GetRow {
	//得到一行数据
	public static void getOneRow(String tableName,String rows){
		try {
			HTable htable = new HTable(CreateHBaseConnection.createHadoopConfiguration(), Bytes.toBytes(tableName));
			Get get = new Get(Bytes.toBytes(rows));
			Result res = htable.get(get);
			Cell[] cells = res.rawCells();
			for(Cell cell : cells){
				String familyName = new String(cell.getFamily(),"utf-8");
				String columnsName = new String(cell.getQualifier(),"utf-8");
				String values = new String(cell.getValue(),"utf-8");
				String rowName = new String(cell.getRow(),"utf-8");
				long timestamp = cell.getTimestamp();
				System.out.println(familyName + "--" + columnsName + "--" + values + "--" + rowName + "--" + timestamp);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	//得到所有数据
	public static void getAllRow(String tableName){
		try {
			HTable htable = new HTable(CreateHBaseConnection.createHadoopConfiguration(), Bytes.toBytes(tableName));
			ResultScanner rs = htable.getScanner(new Scan());
			Iterator<Result> it = rs.iterator();
			while(it.hasNext()){
				Result r = it.next();
				Cell[] cells = r.rawCells();
				for(Cell cell : cells){
					String familyName = new String(cell.getFamily(),"utf-8");
					String columnsName = new String(cell.getQualifier(),"utf-8");
					String values = new String(cell.getValue(),"utf-8");
					String rowName = new String(cell.getRow(),"utf-8");
					long timestamp = cell.getTimestamp();
					System.out.println(familyName + "--" + columnsName + "--" + values + "--" + rowName + "--" + timestamp);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
