package kkr.DIUpdate;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.Scanner;

import kkr.DIUpdate.CommonUtils.DataBaseUtils;
import kkr.DIUpdate.CommonUtils.DateUtils;

public class CSVDataInsertVolatility {

	public static void main(String args[]) throws Exception {

		Connection con=DataBaseUtils.connectLocal();
		String line = null;
		try {
			String csvSplitter = ",";
			String columnSplit = " ";
			BufferedReader br = new BufferedReader(new FileReader(new File("resources/VIX.csv")));
			int i = 0;
			while ((line = br.readLine()) != null) {
				String[] cols = line.split(csvSplitter);
				if (i == 0) {
					i++;
					continue;
				}
				String d_date = cols[0];
				Double Open = Double.parseDouble(cols[1]);
				Double High = Double.parseDouble(cols[2]);
				Double Low = Double.parseDouble(cols[3]);
				Double close = Double.parseDouble(cols[4]);
				Double adjclose = Double.parseDouble(cols[5]);
				
				String SQL_QUERY = "SELECT * FROM Volatility_Index WHERE history_date ='"
						+ d_date + "'";
				Statement checkstatement = con.createStatement();
				ResultSet checkSet = checkstatement.executeQuery(SQL_QUERY);
				int size = 0;
				if (checkSet != null) {
					checkSet.beforeFirst();
					checkSet.last();
					size = checkSet.getRow();
				}

				if (size < 1) {
					System.out.println(SQL_QUERY);
					PreparedStatement pSLocal = con.prepareStatement(
							"insert into Volatility_Index (history_date,Open,High,Low,Close,AdjClose) "
							+ "values (?,?,?,?,?,?)");

					pSLocal.setString(1, d_date);
					pSLocal.setDouble(2, Open);
					
					pSLocal.setDouble(3, High);
					pSLocal.setDouble(4, Low);
					pSLocal.setDouble(5, close);
					pSLocal.setDouble(6, adjclose);
					
					pSLocal.execute();
				}

			}
			br.close();
		} catch (IOException e) {

			System.out.println("exception at reading: " + e);
		}

	}

}
