package online.wangxuan.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class PhoenixTestCase {
	
	public static Connection getConn() {
		try {
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
			return DriverManager.getConnection("jdbc:phoenix");
		} catch (Exception e) {
			System.out.println("获取连接失败！");
			return null;
		}
	}
	
	public static void createTable() {
		Connection conn = null;
		conn = getConn();
		try {
			ResultSet rs = conn.getMetaData().getTables(null, null, "user", null);
			if(rs.next()) {
				System.out.println("已存在！");
				return;
			}
			String sql = "CREATE TABLE user (id varchar PRIMARY KEY,INFO.account varchar ,INFO.passwd varchar)";
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.execute();
			System.out.println("创建表成功");
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if(conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	
	public static void upsertData() {
		Connection conn = getConn();
		String sql = "upsert into user(id, INFO.account, INFO.passwd) values('001', 'admin', 'admin')";
		try {
			PreparedStatement ps = conn.prepareStatement(sql);
			String msg = ps.executeUpdate() > 0 ? "插入成功" : "插入失败！";
			conn.commit();
			System.out.println(msg);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if(conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	
	public static void queryData() {
		Connection conn = getConn();
		String sql = "select * from user";
		try {
			PreparedStatement ps = conn.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();
			System.out.println("id" + "\t" + "account" + "\t" + "passwd");
			System.out.println("======================");
			if(rs != null) {
				while (rs.next()) {
					System.out.print(rs.getString("id") + "\t");
					System.out.print(rs.getString("account") + "\t");
					System.out.println(rs.getString("passwd"));
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if(conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public static void delData() {
		Connection conn = getConn();
		String sql = "delete from user where id = '001'";
		try {
			PreparedStatement ps = conn.prepareStatement(sql);
			String msg = ps.executeUpdate() > 0 ? "删除成功" : "删除失败！";
			conn.commit();
			System.out.println(msg);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if(conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public static void dropTable() {
		Connection conn = getConn();
		String sql = "drop table user"; 
		try {
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.execute();
			System.out.println("drop table success!");
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if(conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public static void main(String[] args) throws SQLException {
		createTable();
	// 	upsertData();
	//	queryData();
	//	delData();
	//	dropTable();
	}
}
