package kkr.DIUpdate;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import kkr.DIUpdate.CommonUtils.DataBaseUtils;

public class EmployeeFeedBackDataLoader {
	
	public static void main(String []args) throws ClassNotFoundException, SQLException{
		
		Connection conkkrProd=DataBaseUtils.connectkkrProd();
		Connection conkkrdev=DataBaseUtils.connectkkrDev();
		Connection con=DataBaseUtils.connectLocal();
		
		ReviewDetailsDataLoader(con);
		//LoadDataintoDev(con);
	}

	private static void ReviewDetailsDataLoader(Connection con) throws SQLException {
		// TODO Auto-generated method stub
		
		String sql_query="SELECT * FROM `review_details` ORDER BY review_details_id";
		
		Statement st=con.createStatement();
		ResultSet rs=st.executeQuery(sql_query);
		while(rs.next()){
			String company_ticker=rs.getString(11);
			String SQL_Check="SELECT * FROM `review_details` where company_ticker='"+company_ticker+"'";
		}
		
	}

}
