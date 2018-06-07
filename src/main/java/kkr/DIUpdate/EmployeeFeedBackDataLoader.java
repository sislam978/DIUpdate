package kkr.DIUpdate;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import kkr.DIUpdate.CommonUtils.DataBaseUtils;

public class EmployeeFeedBackDataLoader {
	
	private static String fileName = "resources/review_detail_log";

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {

		PrintStream ps = new PrintStream(new  FileOutputStream(fileName));
		
		// Connection conkkrProd=DataBaseUtils.connectkkrProd();
		// Connection conkkrdev=DataBaseUtils.connectkkrDev();
		Connection conClient = DataBaseUtils.connectKkrClient();
		Connection con = DataBaseUtils.connectLocal();

		ReviewDetailsDataLoader(con, conClient, ps);
		// LoadDataintoDev(con);
	}

	private static void ReviewDetailsDataLoader(Connection conRead, Connection conWrite, PrintStream ps) throws SQLException, IOException {
		// TODO Auto-generated method stub

		String sql_query = "SELECT * FROM `review_details` ORDER BY review_details_id";

		Statement st = conRead.createStatement();
		ResultSet rs = st.executeQuery(sql_query);
		String sql = "Insert into review_details (rating,title,source,reviewer_site,pros,cons,author,description,date,"
				+ "company_ticker,kkr_company_id,created_on,modified_on) values(?,?,?,?,?,?,?,?,?,?,?,?,?)";
		PreparedStatement pt = conWrite.prepareStatement(sql);

		while (rs.next()) {

			pt.setInt(1, rs.getInt("rating"));
			pt.setString(2, rs.getString("title"));
			pt.setString(3, rs.getString("source"));
			pt.setString(4, rs.getString("reviewer_site"));
			pt.setString(5, rs.getString("pros"));
			pt.setString(6, rs.getString("cons"));
			pt.setString(7, rs.getString("author"));
			pt.setString(8, rs.getString("description"));
			pt.setString(9, rs.getString("date"));
			pt.setString(10, rs.getString("company_ticker"));
			pt.setInt(11, rs.getInt("kkr_company_id"));
			pt.setString(12, rs.getString("created_on"));
			pt.setString(13, rs.getString("modified_on"));

			pt.addBatch();
			ps.println("Review details is batched for company id: " + rs.getInt("review_details_id"));

		}
		System.out.println("Batch is ready to Insert");
		String sqlT = "Truncate table review_details";
		PreparedStatement ptTruncate = conWrite.prepareStatement(sqlT);
		ptTruncate.executeUpdate();
		System.out.println("Table is truncated for insertion.");
		pt.executeBatch();
		System.out.println("Data insertion complete.");

	}

}
