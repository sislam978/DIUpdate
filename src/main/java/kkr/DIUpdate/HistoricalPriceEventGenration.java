package kkr.DIUpdate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import kkr.DIUpdate.CommonUtils.DataBaseUtils;
import kkr.DIUpdate.CommonUtils.DateUtils;

public class HistoricalPriceEventGenration {
	
	public static void main(String args[]) throws ClassNotFoundException, SQLException{
		
		String sql_query="SELECT * FROM `volatility_index` ORDER BY history_date";
		Connection con=DataBaseUtils.connectLocal();
		
		Statement kkrDb=con.createStatement();
		ResultSet rs = kkrDb.executeQuery(sql_query);
		Map<String,String> eventDateMap= new HashMap<String, String>();
		String start_date=null;
		String end_date=null;
		String prev_date=null;
		int i=0;
		while(rs.next()){
			if(rs.getObject(8)==null) continue;
			String d_d=rs.getString(2);
			
			if(rs.getDouble(8)>0.1){
				if(i==0){
					start_date=rs.getString(2);
					i++;
				}
				
				prev_date=rs.getString(2);

			}
			else{
				end_date=prev_date;
				eventDateMap.put(start_date, end_date);
				i=0;
			}
		}
		TreeMap<String, String> sorted = new TreeMap<>(eventDateMap);

		// Copy all data from hashMap into TreeMap
		//sorted.putAll(eventDateMap);
		InsertNewEvent(con,sorted);
		
	}

	private static void InsertNewEvent(Connection con,Map<String, String> eventDateMap) throws SQLException {
		// TODO Auto-generated method stub
		for(Map.Entry<String,String>entry:eventDateMap.entrySet()){
			String sql_query="SELECT * FROM `historical_price_event` where event_type='"+"Volatility Spike"+"' AND "
					+ "start_date='"+entry.getKey()+"' and end_date='"+entry.getValue()+"'";
			int size = 0;
			Statement ss=con.createStatement();
			ResultSet rs=ss.executeQuery(sql_query);
			if (rs != null) {
				rs.beforeFirst();
				rs.last();
				size = rs.getRow();
			}
			if(size<1){
				Statement event_name=con.createStatement();
				ResultSet eve=event_name.executeQuery("SELECT event_name FROM `historical_price_event` where "
						+ "event_type='"+"Volatility Spike"+"' order by historical_price_event_id desc limit 1");
				eve.next();
				PreparedStatement pSLocal = con.prepareStatement(
						"insert into  historical_price_event (event_name,event_type,start_date,end_date) values (?,?,?,?)");
				String []event=eve.getString(1).split(" ");
				int vv=Integer.parseInt(event[2]);
				vv++;
				
				String final_name=null;
				if(vv<100){
					final_name=event[0]+" "+event[1]+" 0"+vv;
				}
				else{
					final_name=event[0]+" "+event[1]+" "+vv;
				}
				pSLocal.setString(1, final_name);
				
				pSLocal.setString(2, "Volatility Spike");
				pSLocal.setString(3, entry.getKey());
				pSLocal.setString(4, entry.getValue());
				pSLocal.execute();
			}
		}
	}

}
