package TDH484Hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
/**
 *创建表，判断表是否存在，动态添加列族
 */
public class CreateTbale {
	public Configuration conf = CreateHBaseConnection.createHadoopConfiguration();
	public HBaseAdmin hadmin ;
	public HTableDescriptor tableDesc ;
	public HColumnDescriptor familyDesc ;
	public boolean tableExists = false;
	
	//创建表，表名、列族名作为数
	public void create(String tableName,List<String> columsFamily){
		try {
			hadmin = new HBaseAdmin(conf);
			//判断表是否存在
			if(isTableExists(tableName)){
				System.out.println("表" + tableName + " 已存在 !");
			}
			tableDesc = new HTableDescriptor(TableName.valueOf(tableName.getBytes()));
			for(String cf : columsFamily){
				familyDesc = new HColumnDescriptor(Bytes.toBytes(cf));
				tableDesc.addFamily(familyDesc);
			}
			hadmin.createTable(tableDesc);
			System.out.println("创建表成功");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	//判断表是否已经存在
	public boolean isTableExists(String tableName){
		try {
			hadmin = new HBaseAdmin(conf);
			tableExists = hadmin.tableExists(tableName.getBytes());
			return tableExists;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return tableExists;
	}
	//动态添加列族
	//这里增加列，但是实际上还是增加了列族，意义是建表后，在之前存在的列族基础上增加列族。
	//在增加列族的时候，列族名称是按照字典排序的，如，创建f1-f10的列族，排序为f1,f10,f2,f3,f4...
	public void addColumnsFamily(String tableName,String columnsFamily){
		try {
			hadmin = new HBaseAdmin(conf);
			familyDesc = new HColumnDescriptor(Bytes.toBytes(columnsFamily));
			hadmin.addColumn(tableName.getBytes(), familyDesc);
			System.out.println("插入列族成功");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
