package com.pateo.telematic.utils;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;

/**
 * 简易版的连接池
 */
public class ConnectionPool implements Serializable{
 
	private static final long serialVersionUID = 1L;
	// 静态的Connection队列    静态方法访问的外部的对象必须是static修饰的
	private static LinkedList<Connection> connectionQueue;
	/**
	 * 加载驱动
	 */
	static {
		try {
			Class.forName("com.mysql.jdbc.Driver");
//			Class.forName("com.mysql.cj.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 获取连接，多线程访问并发控制
	 */
	public synchronized static Connection getConnection() {
		try {
			if (connectionQueue == null) {
				connectionQueue = new LinkedList<Connection>();
				for (int i = 0; i < 10; i++) {
					//Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/obd_message", "root", "root");
					//10.172.10.170
					Connection conn = DriverManager.getConnection("jdbc:mysql://10.172.10.170:3306/obd_message", "obd_message", "obd_message");
					connectionQueue.push(conn);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return connectionQueue.poll();
	}

	/**
	 * 还回去一个连接
	 */
	public static void returnConnection(Connection conn) {
		connectionQueue.push(conn);
	} 
	
	public static void main(String[] args) {
		
	    SimpleDateFormat formatDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		String time = formatDate.format(new Date() ) ;
		System.out.println(" substring " + time.substring(0, 18));
		
		String startTime = time.substring(0, 18) + "0"; 
		
		int count = 100;
		
		String sq = " insert into obd_message.obd_timeseq_count values('" +  startTime+"'," +count +") ";
		System.out.println("append string is " + sq) ;
		
		Connection conn = null;
		String sql = " select * from  obd_message.province where id=?";
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			conn =  ConnectionPool.getConnection();
			
			pstmt = conn.prepareStatement(sq);
			int executeUpdate = pstmt.executeUpdate();
			System.out.println(" return is " + executeUpdate); 
//			pstmt = conn.prepareStatement(sql);
//			pstmt.setInt(1, 2);
//			rs = pstmt.executeQuery();
//			while (rs.next()) {
//				String name = rs.getString(2);
//				System.out.println("name : " + name);
//			}
			ConnectionPool.returnConnection(conn);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
