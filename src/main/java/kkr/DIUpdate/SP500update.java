package kkr.DIUpdate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Scanner;

import kkr.DIUpdate.CommonUtils.DataBaseUtils;
import kkr.DIUpdate.CommonUtils.ReadCSV;

public class SP500update 
{
	
	public static Scanner input;
	
	public static void main(String [] args) throws ClassNotFoundException, SQLException
	{
		ArrayList<String> tickerList = new ArrayList<String>();
		tickerList = makeTickerList();
		
		Connection con = DataBaseUtils.connectLocal();
		
		updateSP500(con,tickerList);
		
		
	}
	
	
	
	public static void updateSP500(Connection con, ArrayList<String> tickerList) 
	{
		try
		{
			// update all the sp500 make to "N"
			String updateSQL = "update kkr_company set sp500 = 'N'";
			PreparedStatement setN = con.prepareStatement(updateSQL);
			setN.executeUpdate();
			
			// update New the sp500 make to "Y"
			for(int j =0; j <tickerList.size(); j++)
			{
				String sql = "UPDATE kkr_company SET sp500 = 'Y' WHERE Company_ticker = '"+tickerList.get(j)+"';";
				PreparedStatement updateY = con.prepareStatement(sql);
				updateY.executeUpdate();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public static ArrayList<String> makeTickerList()
	{
		ArrayList<String> tickerList = new ArrayList<String>();
		
		System.out.print("Enter input file Name: ");
		input = new Scanner(System.in);
		String fileName = input.nextLine();
		
		ArrayList<ArrayList<String>> dataHolder = new ArrayList<ArrayList<String>>();
		dataHolder = ReadCSV.read(fileName);
		
		
		for( int i =0; i < dataHolder.size(); i++)
		{
			ArrayList<String> temp = new ArrayList<String>();
			temp = ReadCSV.splitLine(dataHolder.get(i).get(0), " ");
			
			if(!temp.get(0).equalsIgnoreCase("Ticker"))
			{
				tickerList.add(temp.get(0));
			}
			
		}
		
		return tickerList;
	}

}
