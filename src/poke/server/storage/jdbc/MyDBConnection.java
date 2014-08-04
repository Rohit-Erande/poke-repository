package poke.server.storage.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;

public class MyDBConnection {

	public static Connection getConnection() {
		// TODO Auto-generated method stub
		Logger logger = LoggerFactory.getLogger("server");
		Connection conn = null;
		String URL = "jdbc:mysql://192.168.0.5:3306/mooc";
		String USER = "root";
		String PASS = "admin";
	
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn= DriverManager.getConnection(URL, USER, PASS);
			logger.info("connection done "+conn);
		} catch (Exception e) {
			System.out.println("Exception in est connection"+e);
			e.printStackTrace();
		}
		return conn;		
	}
}

