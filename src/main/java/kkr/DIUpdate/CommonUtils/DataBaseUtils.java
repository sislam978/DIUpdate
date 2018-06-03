package kkr.DIUpdate.CommonUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DataBaseUtils {

	public static Connection connectLocal() throws ClassNotFoundException, SQLException {
		Class.forName("com.mysql.jdbc.Driver");
		Connection con = (Connection) DriverManager
				.getConnection("jdbc:mysql://localhost:3306/kkrdb?rewriteBatchedStatements=true", "root", "");
		con.setAutoCommit(true);
		return con;
	}
	
	public static Connection connectKkrClient() throws ClassNotFoundException, SQLException {
		Class.forName("com.mysql.jdbc.Driver");
		Connection con = (Connection) DriverManager
				.getConnection("jdbc:mysql://localhost:3306/kkrclient?rewriteBatchedStatements=true", "root", "");
		con.setAutoCommit(true);
		return con;
	}
	
	public static Connection connectkkrProd() throws ClassNotFoundException, SQLException {
		Class.forName("com.mysql.jdbc.Driver");
		/*
		 * connection string
		 * "jdbc:mysql://kkrprod.craeiofbogb9.us-west-2.rds.amazonaws.com:3306/kkrdb?rewriteBatchedStatements=true"
		 * ,"kkr_app","kkr123"
		 * 
		 */
		Connection con = (Connection) DriverManager
				.getConnection("jdbc:mysql://kkrprod.craeiofbogb9.us-west-2.rds.amazonaws.com:3306/kkrdb?rewriteBatchedStatements=true","kkr_app","kkr123");
		con.setAutoCommit(true);
		return con;
	}

	public static Connection connectkkrDev() throws ClassNotFoundException, SQLException {
		Class.forName("com.mysql.jdbc.Driver");
		/*
		 * Connection string for kkrdev
		 * "jdbc:mysql://kkrdev.craeiofbogb9.us-west-2.rds.amazonaws.com:3306/kkrdb?rewriteBatchedStatements=true"
		 * ,"kkr_app","kkr123"
		 */
		Connection con = (Connection) DriverManager
				.getConnection("jdbc:mysql://kkrdev.craeiofbogb9.us-west-2.rds.amazonaws.com:3306/kkrdb?rewriteBatchedStatements=true","kkr_app","kkr123");
		con.setAutoCommit(true);
		return con;
	}
}
