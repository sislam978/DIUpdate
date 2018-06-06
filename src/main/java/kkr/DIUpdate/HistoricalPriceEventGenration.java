package kkr.DIUpdate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

import kkr.DIUpdate.CommonUtils.DataBaseUtils;
import kkr.DIUpdate.CommonUtils.DateUtils;

public class HistoricalPriceEventGenration {

	public static void main(String args[]) throws Exception {

		Connection con = DataBaseUtils.connectLocal();
		Connection conKKrProd = DataBaseUtils.connectkkrProd();
		//VolatilityEventCreate(con);
		//TreasuryEventCreate(con);

		System.out.println("enter a date where to start calculating the spy:");
		Scanner input = new Scanner(System.in);
		String d_date = input.nextLine();
		SpyEventCreate(conKKrProd, d_date, con);
	}

	public static void SpyEventCreate(Connection conKkr, String Desired_date, Connection con) throws Exception {
		String SQL_query = "SELECT PRICE_DATE FROM `kkr_price` WHERE PRICE_DATE>='" + Desired_date
				+ "' and KKR_company_ID=616 ORDER BY PRICE_DATE ASC";
		Statement locStat = conKkr.createStatement();

		ResultSet rsL = locStat.executeQuery(SQL_query);
		
		String start_date = null;
		String end_date = null;
		String prev_date = null;
		int i = 0;
		int k = 0;
		Map<String, String> eventDateMap = new HashMap<String, String>();
		while (rsL.next()) {
			if (i == 0) {
				start_date = rsL.getString(1);
				double vv = spyEventCreateProcess(conKkr, rsL.getString(1));
				if (vv < -0.1 && k == 0) {
					boolean flag = updateEvent(start_date, con,"S&P Down");
					if (flag) {
						continue;
					}
					k++;
				}
				i++;
			}
			double vv = spyEventCreateProcess(conKkr, rsL.getString(1));
			if (vv < -0.1) {
				prev_date = rsL.getString(1);
			} else {
				Date d=new SimpleDateFormat("yyyy-MM-dd").parse(end_date);
				Date d1=new SimpleDateFormat("yyyy-MM-dd").parse(start_date);
				if(d.getTime()<d1.getTime()) continue;
				
				end_date = prev_date;
				eventDateMap.put(start_date, end_date);
				i = 0;
			}
		}
		TreeMap<String, String> sorted = new TreeMap<>(eventDateMap);
		InsertNewEvent(con, sorted, "S&P Down");

	}

	private static double spyEventCreateProcess(Connection conKkr, String desired_date) throws SQLException {
		// TODO Auto-generated method stub
		String SQL_QUERY = "SELECT PRICE_CLOSE, PRICE_DATE FROM kkr_price where PRICE_DATE<='" + desired_date
				+ "' ORDER BY PRICE_DATE DESC LIMIT 23";

		Statement statement = conKkr.createStatement();

		ResultSet rs = statement.executeQuery(SQL_QUERY);
		ArrayList<Double> closeList = new ArrayList<Double>();
		double oriClose = -1;
		while (rs.next()) {
			if (rs.getString(2).equals(desired_date)) {
				oriClose = rs.getDouble(1);
			}
			closeList.add(rs.getDouble(1));
		}
		Collections.sort(closeList);
		double vv = oriClose / closeList.get(closeList.size() - 1) - 1;
		return vv;
	}

	public static void TreasuryEventCreate(Connection con) throws Exception {
		String sql_query = "SELECT * FROM `treasury_yield_curve_rates` ORDER BY rates_date";

		Statement kkrDb = con.createStatement();
		ResultSet rs = kkrDb.executeQuery(sql_query);
		Map<String, String> eventDateMap = new HashMap<String, String>();
		String start_date = null;
		String end_date = null;
		String prev_date = null;
		int i = 0;
		int k = 0;
		while (rs.next()) {
			if (rs.getObject(6) == null || rs.getObject(11) == null)
				continue;
			double yr1 = rs.getDouble(6);
			double yr10 = rs.getDouble(11);
			double diff = yr1 - yr10;
			if (diff < 0) {
				if (i == 0) {
					start_date = rs.getString(2);
					if (k == 0) {
						boolean flag = updateEvent(start_date, con,"Treasury Inverse");
						if (flag) {
							continue;
						}
						k++;
					}
					i++;
				}
				prev_date = rs.getString(2);
			} else {
				end_date = prev_date;
				eventDateMap.put(start_date, end_date);
				i = 0;
			}
		}
		TreeMap<String, String> sorted = new TreeMap<>(eventDateMap);
		InsertNewEvent(con, sorted, "Treasury Inverse");
	}

	public static void VolatilityEventCreate(Connection con) throws Exception {

		String sql_query = "SELECT * FROM `volatility_index` ORDER BY history_date";

		Statement kkrDb = con.createStatement();
		ResultSet rs = kkrDb.executeQuery(sql_query);
		Map<String, String> eventDateMap = new HashMap<String, String>();
		String start_date = null;
		String end_date = null;
		String prev_date = null;
		int i = 0;
		int k = 0;
		while (rs.next()) {
			if (rs.getObject(8) == null)
				continue;
			String d_d = rs.getString(2);

			if (rs.getDouble(8) > 0.1) {
				if (i == 0) {
					start_date = rs.getString(2);
					if (k == 0) {
						boolean flag = updateEvent(start_date, con,"Volatility Spike");
						if (flag) {
							continue;
						}
						k++;
					}
					i++;
				}

				prev_date = rs.getString(2);

			} else {
				end_date = prev_date;
				eventDateMap.put(start_date, end_date);
				i = 0;
			}
		}
		TreeMap<String, String> sorted = new TreeMap<>(eventDateMap);
		// Copy all data from hashMap into TreeMap
		// sorted.putAll(eventDateMap);
		InsertNewEvent(con, sorted, "Volatility Spike");
	}

	private static boolean updateEvent(String start_date, Connection con,String event_type) throws SQLException, ParseException {
		// TODO Auto-generated method stub
		Calendar prev = Calendar.getInstance();
		prev.setTime(new SimpleDateFormat("yyyy-MM-dd").parse(start_date));
		prev.add(Calendar.DATE, -1); // number of days to add
		String prev_date = new SimpleDateFormat("yyyy-MM-dd").format(prev.getTime());
		String SQL_QUERY = "SELECT * FROM `historical_price_event` where end_date='" + prev_date + "' and event_type='"+event_type+"'";

		Statement st = con.createStatement();
		ResultSet rs = st.executeQuery(SQL_QUERY);
		int size = 0;
		if (rs != null) {
			rs.beforeFirst();
			rs.last();
			size = rs.getRow();
		}
		if (size < 1)
			return false;
		else {
			String query_str = "UPDATE historical_price_event SET end_date='" + start_date + "' WHERE end_date='"
					+ prev_date + "'";
			Statement update_statement = con.createStatement();
			update_statement.executeUpdate(query_str);
		}
		return true;
	}

	private static void InsertNewEvent(Connection con, Map<String, String> eventDateMap, String event_type)
			throws Exception {
		// TODO Auto-generated method stub
		for (Map.Entry<String, String> entry : eventDateMap.entrySet()) {
			String sql_query = "SELECT * FROM `historical_price_event` where event_type='" + event_type + "' AND "
					+ "start_date='" + entry.getKey() + "' and end_date='" + entry.getValue() + "'";
			int size = 0;
			Statement ss = con.createStatement();
			ResultSet rs = ss.executeQuery(sql_query);
			if (rs != null) {
				rs.beforeFirst();
				rs.last();
				size = rs.getRow();
			}
			if (size < 1) {
				String Sql_stat = null;
				String final_name = null;
				PreparedStatement pSLocal = con.prepareStatement(
						"insert into  historical_price_event (event_name,event_type,start_date,end_date) values (?,?,?,?)");

				if (event_type.equals("Volatility Spike")) {
					Sql_stat = "SELECT event_name FROM `historical_price_event` where event_type='" + event_type
							+ "' order by historical_price_event_id desc limit 1";
					Statement event_name = con.createStatement();
					ResultSet eve = event_name.executeQuery(Sql_stat);
					eve.next();
					String[] event = eve.getString(1).split(" ");
					int vv = Integer.parseInt(event[2]);
					vv++;
					if (vv < 100) {
						final_name = event[0] + " " + event[1] + " 0" + vv;
					} else {
						final_name = event[0] + " " + event[1] + " " + vv;
					}
				} else if (event_type.equals("Treasury Inverse")) {
					String d_date = DateUtils.stringTodate(entry.getKey(), "yyyy-MM-dd", "MMM d, yyyy");
					String d_array[] = d_date.split(",|\\s");
					final_name = event_type + d_array[0] + d_array[3];
				} else if (event_type.equals("S&P Down")) {
					Sql_stat = "SELECT event_name FROM `historical_price_event` where event_type='" + event_type
							+ "' order by historical_price_event_id desc limit 1";
					Statement event_name = con.createStatement();
					ResultSet eve = event_name.executeQuery(Sql_stat);
					eve.next();
					String[] event = eve.getString(1).split(" ");
					int vv = Integer.parseInt(event[2]);
					vv++;
					if (vv < 10) {
						final_name = event[0] + " 0" + vv;
					} else {
						final_name = event[0] + " " + vv;
					}
				}
				pSLocal.setString(1, final_name);
				pSLocal.setString(2, event_type);
				pSLocal.setString(3, entry.getKey());
				pSLocal.setString(4, entry.getValue());
				pSLocal.execute();
			}
		}
	}

}
