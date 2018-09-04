package TDH484Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class CreateHBaseConnection {
	
	//创建Hadoop的配置，Configuration
	public static Configuration createHadoopConfiguration(){
		Configuration conf = new Configuration();
		conf.addResource("core-site.xml");
		conf.addResource("hdfs-site.xml");
		conf.addResource("hbase-site.xml");
		return conf;
	}
	
	//创建HBase的HbaseConfiguration，但实际上还是用的Hadoop的Configuration
	public static Configuration createHBaseConfiguration(){
		Configuration hconf = HBaseConfiguration.create();
		hconf.addResource("hbase-site.xml");
		return hconf;
	}
	
}
