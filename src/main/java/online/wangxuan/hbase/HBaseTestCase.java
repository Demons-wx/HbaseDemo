package online.wangxuan.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTestCase {
	public static Configuration conf;
	public static Connection connection;
	public static Admin admin;
	
	public static void main(String[] args) throws IOException {
	//	createTable("USER", new String[]{"age","name"});
	//	listTable();
	//	insertRow("USER", "wx", "name", "firstname", "wang");
	//	insertRow("user", "wx", "age", "", "20");
		
	//	getData("user", "wx", "name", "firstname");
	//	delData("user", "wx", "name", "firstname");
		
	//	getData("user", "wx", "", "");
	//	scanData("user", "", "");
	//	delTable("USER");
	//	scanData("employee","","");
		getData("employee", "row1", "company", "name");
	}
	
	public static void init() {
		conf = HBaseConfiguration.create();
		try {
			connection = ConnectionFactory.createConnection(conf);
			admin = connection.getAdmin();
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
	}
	
	public static void close() {
		try {
			if(admin != null)
				admin.close();
			if(connection != null)
				connection.close();
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
	}
	
	@SuppressWarnings("deprecation")
	public static void createTable(String tableName, String[] cols) throws IOException {
		init();
		TableName tbName = TableName.valueOf(tableName);
		if(admin.tableExists(tbName)) {
			System.out.println("table exists!");
		} else {
			HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
			for (String col : cols) {
				HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
				hTableDescriptor.addFamily(hColumnDescriptor);
			}
			admin.createTable(hTableDescriptor);
		}
		close();
	}
	
	public static void listTable() throws IOException {
		init();
		HTableDescriptor[] hTableDescriptors = admin.listTables();
		for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
			System.out.println(hTableDescriptor.getNameAsString());
		}
	}
	
	public static void insertRow(String tableName, String rowkey, String colFamily, 
			String col, String val) throws IOException {
		init();
		Table table = connection.getTable(TableName.valueOf(tableName));
		Put put = new Put(Bytes.toBytes(rowkey));
		
		put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
		table.put(put);
		table.close();
		close();
	}
	
	public static void getData(String tableName, String rowkey, 
			String colFamily, String col) throws IOException {
		init();
		Table table = connection.getTable(TableName.valueOf(tableName));
		Get get = new Get(Bytes.toBytes(rowkey));
		// 获取指定列簇数据
		get.addFamily(Bytes.toBytes(colFamily));
		// 获取指定列数据
		get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
		Result result = table.get(get);
		showCell(result);
		table.close();
		close();
	}
	
	public static void showCell(Result result) {
		Cell[] cells = result.rawCells();
		for (Cell cell : cells) {
			System.out.println("RowName: " + new String(CellUtil.cloneRow(cell)));
			System.out.println("Timestamp: " + cell.getTimestamp());
			System.out.println("column Family: " + new String(CellUtil.cloneFamily(cell)));
			System.out.println("column name: " + new String(CellUtil.cloneQualifier(cell)));
			System.out.println("value: " + new String(CellUtil.cloneValue(cell)));
		}
	}
	
	public static void delData(String tableName, String rowkey, 
			String colFamily, String col) throws IOException {
		init();
		Table table = connection.getTable(TableName.valueOf(tableName));
		Delete delete = new Delete(Bytes.toBytes(rowkey));
		// 删除指定列簇
		delete.addFamily(Bytes.toBytes(colFamily));
		// 删除指定列
		delete.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
		table.delete(delete);
		table.close();
		close();
	}
	
	public static void scanData(String tableName, String startRow, String stopRow) throws IOException {
		init();
		Table table = connection.getTable(TableName.valueOf(tableName));
		Scan scan = new Scan();
		ResultScanner resultScanner = table.getScanner(scan);
		for (Result result : resultScanner) {
			showCell(result);
		}
		close();
	}
	
	public static void delTable(String tableName) throws IOException {
		init();
		Table table = connection.getTable(TableName.valueOf(tableName));
		admin.disableTable(table.getName());
		admin.deleteTable(table.getName());
		System.out.println("deleted table " + tableName);
		table.close();
		close();
	}
}






















