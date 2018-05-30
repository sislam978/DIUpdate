package kkr.DIUpdate;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import kkr.DIUpdate.CommonUtils.DataBaseUtils;
import kkr.DIUpdate.CommonUtils.DateUtils;

public class PortfolioTimeSeriesManager {

	public static final int Y1 = 122;
	public static final int Y2 = 123;
	public static final int Y3 = 124;
	public static final int Y5 = 125;
	public static final int Y7 = 126;
	public static final int Y10 = 127;
	public static final int Y20 = 128;
	public static final int Y30 = 129;

	public static final Map<Integer, Integer> percentIndexMap = new HashMap<Integer, Integer>();
	public static ArrayList<Integer> YieldIndex = new ArrayList<Integer>();

	public static void main(String[] args) throws Exception {

		YieldIndex.add(Y1);
		YieldIndex.add(Y2);
		YieldIndex.add(Y3);
		YieldIndex.add(Y5);
		YieldIndex.add(Y7);
		YieldIndex.add(Y10);
		YieldIndex.add(Y20);
		YieldIndex.add(Y30);

		percentIndexMap.put(131, 5);
		percentIndexMap.put(132, 10);
		percentIndexMap.put(133, 15);
		percentIndexMap.put(134, 20);

		Date dd = new Date();
		System.out.println(dd.toString());
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		String desired_date = dateFormat.format(dd);
		System.out.println(desired_date);
		Connection conL = DataBaseUtils.connectLocal();

//		String SQL_query = "SELECT rates_date FROM `treasury_yield_curve_rates` ORDER BY rates_date ASC";
//		Statement locStat = conL.createStatement();
//
//		ResultSet rsL = locStat.executeQuery(SQL_query);
//		while (rsL.next()) {
//			String d_date = rsL.getString(1);
//			insertData(d_date);
//			inserDataForPercents(d_date);
//			updatePortfolioTimeSeriePercents(d_date);
//		}
		
		String SQL_query2 = "SELECT history_date from volatility_index  ORDER BY history_date ASC";
		Statement locStat1 = conL.createStatement();

		ResultSet rsL1 = locStat1.executeQuery(SQL_query2);
		while(rsL1.next()){
			String d_date=rsL1.getString(1);
			insertVolatilityData(d_date);
		}

	}

	public static void insertData(String desired_date) throws Exception {

		Connection conL = DataBaseUtils.connectLocal();
		Connection conKkr = DataBaseUtils.connectKkrClient();
		String sql = "SELECT * FROM `user_saved_portfolio_timeseries` WHERE timeseries_date='" + desired_date + "'";
		Statement check_st = conKkr.createStatement();
		ResultSet checkrsl = check_st.executeQuery(sql);
		int size = 0;
		if (checkrsl != null) {
			checkrsl.beforeFirst();
			checkrsl.last();
			size = checkrsl.getRow();
		}
		if (size < 1) {
			String SQL_query = "SELECT * FROM `treasury_yield_curve_rates` WHERE rates_date='" + desired_date
					+ "' ORDER BY rates_date ASC";

			Statement locStat = conL.createStatement();

			ResultSet rsL = locStat.executeQuery(SQL_query);
			/*
			 * Yr1,Yr2,Yr3,Yr5,Yr7,Yr10,Yr20,Yr30 index value from
			 * user_saved_portfolio table write as a constant because it would
			 * never change. create a arraylist from the constant further
			 * convenience
			 */

			int mm = 0;
			while (rsL.next()) {
				/*
				 * values of Yr1, Yr2, Yr3,Yr5,Yr7,Yr10,Yr20,Yr30
				 */
				ArrayList<Double> YieldValues = new ArrayList<Double>();
				ArrayList<Double> Yield_return = new ArrayList<Double>();
				YieldValues.add(rsL.getDouble(6));
				YieldValues.add(rsL.getDouble(7));
				YieldValues.add(rsL.getDouble(8));
				YieldValues.add(rsL.getDouble(9));
				YieldValues.add(rsL.getDouble(10));
				YieldValues.add(rsL.getDouble(11));
				YieldValues.add(rsL.getDouble(12));
				YieldValues.add(rsL.getDouble(13));

				/*
				 * insert satement create
				 */

				PreparedStatement pSLocal = conKkr.prepareStatement(
						"insert into  user_saved_portfolio_timeseries (user_saved_portfolio_id,timeseries_date,returns,close) values (?,?,?,?)");
				for (int j = 0; j < YieldValues.size(); j++) {

					pSLocal.setInt(1, YieldIndex.get(j));
					pSLocal.setString(2, DateUtils.stringTodate(rsL.getString(2), "yyyy-MM-dd", "MM/dd/yyyy"));
					pSLocal.setDouble(3, YieldValues.get(j));
					pSLocal.setDouble(4, YieldValues.get(j));

					pSLocal.execute();
					System.out.println(mm++);
				}
			}
		}

	}

	private static void inserDataForPercents(String desired_date) throws Exception {
		// TODO Auto-generated method stub

		Connection conKkr = DataBaseUtils.connectKkrClient();
		String sql = "SELECT * FROM `user_saved_portfolio_timeseries` WHERE timeseries_date='" + desired_date + "'";
		Statement check_st = conKkr.createStatement();
		ResultSet checkrsl = check_st.executeQuery(sql);
		int size = 0;
		if (checkrsl != null) {
			checkrsl.beforeFirst();
			checkrsl.last();
			size = checkrsl.getRow();
		}
		if (size < 1) {
			PreparedStatement pSLocal = conKkr.prepareStatement(
					"insert into  user_saved_portfolio_timeseries (user_saved_portfolio_id,timeseries_date,returns) values (?,?,?)");
			int mm = 0;

			for (Map.Entry<Integer, Integer> entry : percentIndexMap.entrySet()) {
				double vv = 1 + ((entry.getValue() * 1.00) / 100);
				double p = 1.00 / 252;
				double pow_val = Math.pow(vv, p);
				double return_val = pow_val - 1;

				pSLocal.setInt(1, entry.getKey());
				pSLocal.setString(2, DateUtils.stringTodate(desired_date, "yyyy-MM-dd", "MM/dd/yyyy"));
				pSLocal.setDouble(3, return_val);

				pSLocal.execute();
				System.out.println(mm++);

			}
		}

	}

	public static void insertVolatilityData(String desired_date) throws ClassNotFoundException, SQLException {
		Connection conL = DataBaseUtils.connectLocal();
		Connection conKkr = DataBaseUtils.connectKkrClient();
//		String sql = "SELECT * FROM `user_saved_portfolio_timeseries` WHERE timeseries_date='" + desired_date + "'";
//		Statement check_st = conKkr.createStatement();
//		ResultSet checkrsl = check_st.executeQuery(sql);
//		int size = 0;
//		if (checkrsl != null) {
//			checkrsl.beforeFirst();
//			checkrsl.last();
//			size = checkrsl.getRow();
//		}
//		if (size < 1) {
			String SQL_query = "SELECT * FROM `volatility_index` WHERE history_date='" + desired_date
					+ "' ORDER BY history_date ASC";

			Statement locStat = conL.createStatement();
			ResultSet rsL = locStat.executeQuery(SQL_query);
			int mm = 0;
			while (rsL.next()) {

				PreparedStatement pSLocal = conKkr.prepareStatement(
						"insert into  user_saved_portfolio_timeseries (user_saved_portfolio_id,timeseries_date,returns,close) values (?,?,?,?)");

				pSLocal.setInt(1, 121);
				double rt = rsL.getDouble(9);
				double close = rsL.getDouble(6);
				pSLocal.setString(2, desired_date);
				pSLocal.setDouble(3, rt);
				pSLocal.setDouble(4, close);

				pSLocal.execute();
				System.out.println(mm++);

			}
		//}
	}

	public static void updatePortfolioTimeSeriePercents(String desired_date) throws Exception {
		Connection conKkr = DataBaseUtils.connectKkrClient();

		String cc_date = DateUtils.stringTodate(desired_date, "yyyy-MM-dd", "MM/dd/yyyy");
		for (Map.Entry<Integer, Integer> entry : percentIndexMap.entrySet()) {

			String SQL_query = "SELECT * FROM user_saved_portfolio_timeseries WHERE timeseries_date>='" + desired_date
					+ "' " + "and user_saved_portfolio_id=" + entry.getKey() + " ORDER BY timeseries_date ASC";

			Statement kkrStat = conKkr.createStatement();

			ResultSet rsKkr = kkrStat.executeQuery(SQL_query);
			int i = 0;
			double prev_close = -1;
			int mm = 0;
			while (rsKkr.next()) {
				if (i == 0) {
					prev_close = rsKkr.getDouble(5);
					i++;
					continue;
				}
				double calculated_close = prev_close * (rsKkr.getDouble(4) + 1);
				String time_seriesdate = rsKkr.getString(3);
				String query_str = "UPDATE user_saved_portfolio_timeseries SET close='" + calculated_close
						+ "' WHERE timeseries_date='" + time_seriesdate + "'";
				Statement update_statement = conKkr.createStatement();
				update_statement.executeUpdate(query_str);
				prev_close = calculated_close;
				System.out.println(mm++);
			}
		}

	}

}
