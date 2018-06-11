package kkr.DIUpdate;

import java.net.Inet4Address;
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

	/*
	 * This ar currently not in use but if we want to use the string to match in
	 * every user id selection for treasury yield then use the following list of
	 * constants
	 */
	// public static final String Y1 = "1Y";
	// public static final String Y2 = "2Y";
	// public static final String Y3= "3Y";
	// public static final String Y5 = "5Y";
	// public static final String Y7 = "7Y";
	// public static final String Y10 = "10Y";
	// public static final String Y20 = "20Y";
	// public static final String Y30 = "30Y";
	// public static final String Y10_Y1="10Y-1Y";

	public static void main(String[] args) throws Exception {

		Scanner input = new Scanner(System.in);
		System.out.println("Enter the start date to insert data:");
		String start_date = input.nextLine();

		System.out.println("Last date of data insertion:");
		String end_date = input.nextLine();

		Connection conL = DataBaseUtils.connectLocal();
		Connection conKkr = DataBaseUtils.connectkkrDevClient();
		Connection conkkrProd = DataBaseUtils.connectkkrProdClient();
		ArrayList<Integer> YindexDev = createStatcElements(conKkr);
		Map<Integer, Integer> percentIndexMapDev = PercentIndexfetch(conKkr);

		ArrayList<Integer> YindexProd = createStatcElements(conkkrProd);
		Map<Integer, Integer> percentIndexMapProd = PercentIndexfetch(conkkrProd);

		String SQL_query = "SELECT rates_date FROM `treasury_yield_curve_rates` where rates_date>='" + start_date
				+ "' and rates_date<='" + end_date + "'ORDER BY rates_date ASC";
		Statement locStat = conL.createStatement();
		ResultSet rsL = locStat.executeQuery(SQL_query);
		int i = 0;
		String prev_date = null;
		while (rsL.next()) {
			String d_date = rsL.getString(1);
			insertData(d_date, conL, conKkr, YindexDev);
			inserDataForPercents(d_date, conKkr, percentIndexMapDev);

			insertData(d_date, conL, conKkr, YindexProd);
			inserDataForPercents(d_date, conKkr, percentIndexMapProd);
			if (i == 0) {
				prev_date = d_date;
				i++;
				continue;
			}
			updatePortfolioTimeSeriePercents(d_date, prev_date, conKkr, percentIndexMapDev);
			updatePortfolioTimeSeriePercents(d_date, prev_date, conKkr, percentIndexMapProd);
			prev_date = d_date;
		}

		String SQL_query2 = "SELECT history_date from volatility_index  where history_date >= '" + start_date
				+ "' and history_date <='" + end_date + "' ORDER BY history_date ASC";
		Statement locStat1 = conL.createStatement();

		int volatality_index_dev = fetchVolIndex(conKkr);
		int volatality_index_prod = fetchVolIndex(conkkrProd);
		ResultSet rsL1 = locStat1.executeQuery(SQL_query2);
		while (rsL1.next()) {
			String d_date = rsL1.getString(1);
			insertVolatilityData(d_date, conL, conKkr, volatality_index_dev);
			insertVolatilityData(d_date, conL, conKkr, volatality_index_prod);
		}

	}

	private static int fetchVolIndex(Connection con) throws SQLException {
		// TODO Auto-generated method stub
		String sql_query = "SELECT user_saved_portfolio_id, name FROM `user_saved_portfolio` WHERE name like '%Annual Return%' order by user_id";
		Statement vis = con.createStatement();
		ResultSet rsvi = vis.executeQuery(sql_query);
		rsvi.next();
		int val = rsvi.getInt(1);
		return val;
	}

	private static Map<Integer, Integer> PercentIndexfetch(Connection con) throws SQLException {
		// TODO Auto-generated method stub
		Map<Integer, Integer> percentIndexMap = new HashMap<Integer, Integer>();
		ArrayList<Integer> indexArary = new ArrayList<Integer>();
		String sql_query = "SELECT user_saved_portfolio_id, name FROM `user_saved_portfolio` WHERE name like '%Annual Return%' order by user_id";
		Statement tys = con.createStatement();
		ResultSet rsTy = tys.executeQuery(sql_query);
		while (rsTy.next()) {
			int index_val = rsTy.getInt(1);
			indexArary.add(index_val);
		}
		percentIndexMap.put(indexArary.get(0), 5);
		percentIndexMap.put(indexArary.get(1), 10);
		percentIndexMap.put(indexArary.get(2), 15);
		percentIndexMap.put(indexArary.get(2), 20);
		return percentIndexMap;

	}

	public static ArrayList<Integer> createStatcElements(Connection con) throws SQLException {
		ArrayList<Integer> YieldIndex = new ArrayList<Integer>();
		String sql_query = "SELECT user_saved_portfolio_id, name FROM `user_saved_portfolio` WHERE name like '%Treasury Yield%' order by user_id";
		Statement tys = con.createStatement();
		ResultSet rsTy = tys.executeQuery(sql_query);
		while (rsTy.next()) {
			int index_val = rsTy.getInt(1);
			YieldIndex.add(index_val);
		}
		return YieldIndex;
	}

	/*
	 * Treasury Yield data insert into time series table
	 */
	public static void insertData(String desired_date, Connection conL, Connection conKkr,
			ArrayList<Integer> YieldIndex) throws Exception {

		String sql = "SELECT * FROM `user_saved_portfolio_timeseries` WHERE user_saved_portfolio_id BETWEEN '"
				+ YieldIndex.get(0) + "' and '" + YieldIndex.get(YieldIndex.size() - 1) + "' and timeseries_date='"
				+ desired_date + "'";
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
				YieldValues.add(rsL.getDouble(13) - rsL.getDouble(6));
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

	/*
	 * Insert Percent data into time series table
	 */
	private static void inserDataForPercents(String desired_date, Connection conKkr,
			Map<Integer, Integer> percentIndexMap) throws Exception {
		// TODO Auto-generated method stub

		String sql = "SELECT * FROM `user_saved_portfolio_timeseries`  WHERE " + "user_saved_portfolio_id BETWEEN '"
				+ percentIndexMap.get(0) + "' and '" + percentIndexMap.get(percentIndexMap.size() - 1)
				+ "' and timeseries_date='" + desired_date + "'";
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
					"insert into  user_saved_portfolio_timeseries (user_saved_portfolio_id,timeseries_date,returns,close) values (?,?,?,?)");
			int mm = 0;

			for (Map.Entry<Integer, Integer> entry : percentIndexMap.entrySet()) {
				double vv = 1 + ((entry.getValue() * 1.00) / 100);
				double p = 1.00 / 252;
				double pow_val = Math.pow(vv, p);
				double return_val = pow_val - 1;

				pSLocal.setInt(1, entry.getKey());
				pSLocal.setString(2, DateUtils.stringTodate(desired_date, "yyyy-MM-dd", "MM/dd/yyyy"));
				pSLocal.setDouble(3, return_val);
				pSLocal.setDouble(4, -1);

				pSLocal.execute();
				System.out.println(mm++);

			}
		}

	}

	/*
	 * volatility index data
	 */
	public static void insertVolatilityData(String desired_date, Connection conL, Connection conKkr,
			int volatality_index) throws Exception {

		String sql = "SELECT * FROM `user_saved_portfolio_timeseries` WHERE user_saved_portfolio_id='"
				+ volatality_index + "' and timeseries_date='" + desired_date + "'";
		Statement check_st = conKkr.createStatement();
		ResultSet checkrsl = check_st.executeQuery(sql);
		int size = 0;
		if (checkrsl != null) {
			checkrsl.beforeFirst();
			checkrsl.last();
			size = checkrsl.getRow();
		}
		if (size < 1) {
			String SQL_query = "SELECT * FROM `volatility_index` WHERE history_date='" + desired_date
					+ "' ORDER BY history_date ASC";

			Statement locStat = conL.createStatement();
			ResultSet rsL = locStat.executeQuery(SQL_query);
			int mm = 0;
			while (rsL.next()) {

				PreparedStatement pSLocal = conKkr.prepareStatement(
						"insert into  user_saved_portfolio_timeseries (user_saved_portfolio_id,timeseries_date,returns,close) values (?,?,?,?)");

				pSLocal.setInt(1, volatality_index);
				double rt = rsL.getDouble(9);
				double close = rsL.getDouble(6);
				pSLocal.setString(2, DateUtils.stringTodate(desired_date, "yyyy-MM-dd", "MM/dd/yyyy"));
				pSLocal.setDouble(3, rt);
				pSLocal.setDouble(4, close);

				pSLocal.execute();
				System.out.println(mm++);

			}
		}
	}

	/*
	 * time series close calculation and update the rows in time series table
	 */
	public static void updatePortfolioTimeSeriePercents(String desired_date, String previousdate, Connection conKkr,
			Map<Integer, Integer> percentIndexMap) throws Exception {

		String cc_date = DateUtils.stringTodate(desired_date, "yyyy-MM-dd", "MM/dd/yyyy");

		/*
		 * previous day data grab
		 */

		String prev_date = DateUtils.stringTodate(previousdate, "yyyy-MM-dd", "MM/dd/yyyy");

		for (Map.Entry<Integer, Integer> entry : percentIndexMap.entrySet()) {

			String SQL_query = "SELECT * FROM user_saved_portfolio_timeseries WHERE timeseries_date='" + cc_date
					+ "'and user_saved_portfolio_id=" + entry.getKey() + "";

			String prev_query = "SELECT * FROM user_saved_portfolio_timeseries WHERE timeseries_date='" + prev_date
					+ "'and user_saved_portfolio_id=" + entry.getKey() + "";

			Statement kkrStat = conKkr.createStatement();
			ResultSet rsKkr = kkrStat.executeQuery(SQL_query);
			rsKkr.next();
			Statement prev_state = conKkr.createStatement();
			ResultSet prevRsl = prev_state.executeQuery(prev_query);
			prevRsl.next();
			// int i = 0;
			double prev_close = prevRsl.getDouble(5);

			double id_prev = prevRsl.getDouble(2);
			double id = rsKkr.getDouble(2);

			double calculated_close = prev_close * (rsKkr.getDouble(4) + 1);
			String time_seriesdate = rsKkr.getString(3);
			System.out.println("id: " + id + " id2: " + id_prev + " calculated close: " + calculated_close
					+ " prev_value: " + prev_close + " date: " + time_seriesdate);
			String query_str = "UPDATE user_saved_portfolio_timeseries SET user_saved_portfolio_timeseries.close='"
					+ calculated_close + "' WHERE timeseries_date='" + time_seriesdate
					+ "' and user_saved_portfolio_id=" + id + "";
			Statement update_statement = conKkr.createStatement();
			update_statement.executeUpdate(query_str);

		}

	}

}
