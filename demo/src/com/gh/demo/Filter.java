package com.gh.demo;

import java.io.IOException;
import java.util.Iterator;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PageFilter;

import com.gh.demo.HBasePrintUtil;;

public class Filter {
   
	public final static String ZK_CONNECT_KEY="hbase.zookeeper.quorum";
	public final static String ZK_CONNECT_VALUE="hadoop02:2181,hadoop03:2181,hadoop04:2181";
	static Configuration config = null;
    static Connection conn = null;
	static Admin admin = null;
	static Table table = null;
	 public static void main(String[] args) throws Exception {		
//		admin = conn.getAdmin();	
//		Scan scan = new Scan();
//		scan.addFamily("cf1".getBytes());
//		scan.addColumn("cf1".getBytes(), "name".getBytes());
//		PageFilter filter = new PageFilter(3);
//		scan.setFilter(filter);
//		scan.setStartRow("rk02".getBytes());
//		ResultScanner scanner = table.getScanner(scan);
		 long start=System.currentTimeMillis();
		HBasePrintUtil.printResultScanner(page(2,2));
		long end = System.currentTimeMillis();
		System.out.println(end - start);
	}
	 public static ResultScanner page(String startRow,int pageSize) throws IOException{
		 config = HBaseConfiguration.create();
		 config.set(ZK_CONNECT_KEY, ZK_CONNECT_VALUE);
		 conn = ConnectionFactory.createConnection(config);
		 Scan scan = new Scan();
		 scan.setStartRow(startRow.getBytes());
		 PageFilter filter = new PageFilter(pageSize);
		 table = conn.getTable(TableName.valueOf("irving11"));
		 scan.setFilter(filter);
		 ResultScanner resultScanner = table.getScanner(scan);
		 return resultScanner;
	 }
	 public static ResultScanner page(int start,int pageSize) throws Exception{
//		config = HBaseConfiguration.create();
//		config.set(ZK_CONNECT_KEY, ZK_CONNECT_VALUE);
//		conn = ConnectionFactory.createConnection(config);
//		table = conn.getTable(TableName.valueOf("irving11"));
		if(start < 1){
			start = 1;
		}
		if(start == 1){
			return page("", pageSize);
		}
//		Scan scan = new Scan();
		ResultScanner resultScanner = page(start-1, pageSize+1);
		Result result = null;
		Iterator<Result> iterator = resultScanner.iterator();
		while(iterator.hasNext()){
			result = iterator.next();
		}
		String startRow = new String(result.getRow());
		return page(startRow, pageSize);
	 }
}
