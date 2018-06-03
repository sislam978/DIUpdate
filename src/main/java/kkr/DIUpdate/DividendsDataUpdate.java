package kkr.DIUpdate;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.RoundingMode;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import com.mysql.fabric.xmlrpc.base.Array;

import kkr.DIUpdate.CommonUtils.DataBaseUtils;

public class DividendsDataUpdate {
	public static final ArrayList<String> groupName = new ArrayList<String>();


	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		InsertGroupNamesinList();
		Scanner in = new Scanner(System.in);
		System.out.println("Insert desired date with format YYYY-mm-DD to find all the data for the groups: ");
		String d_date = in.nextLine();
		try {

			Connection con = DataBaseUtils.connectkkrProd();
			Connection conkkrDev = DataBaseUtils.connectkkrDev();

			Statement sCon = con.createStatement();

			int i = 1;
			/*
			 * file create to maintain log
			 */
			String perent_directory = "resources/";
			String[] splitdate = d_date.split("-");
			String insert_fileName = "insert_Log" + splitdate[0] + splitdate[1] + splitdate[2];
			String duplicate_log = "duplicate_log" + splitdate[0] + splitdate[1] + splitdate[2];
			String file_path = perent_directory + insert_fileName;
			String duplicate_filePath = perent_directory + duplicate_log;
			File insertLog = new File(file_path);
			File dupLog = new File(duplicate_filePath);
			
			BufferedWriter bw_insert = new BufferedWriter(new FileWriter(insertLog));
	        BufferedWriter bw_dup = new BufferedWriter(new FileWriter(dupLog));
	        
			for (int g = 0; g < groupName.size(); g++) {
				System.out.println("looping: " + i++);

				String group = groupName.get(g);
				JSONObject jsonObject = getResultsFromQM(group, d_date);
				// System.out.println(jsonObject);

				JSONObject json1 = (JSONObject) jsonObject.get("results");

				JSONArray dividends = json1.has("dividends") ? json1.getJSONArray("dividends") : null;

				if (dividends != null) {
					System.out.println("dividends size: " + dividends.length());
					for (int j = 0; j < dividends.length(); j++) {
						JSONObject dividendObject = dividends.getJSONObject(j);
						// System.out.println(dividendObject);
						if (dividendObject != null) {
							JSONArray dividendArr = dividendObject.has("dividend")
									? dividendObject.getJSONArray("dividend") : null;
							JSONObject dividendKey = dividendObject.getJSONObject("key");

							Statement companyStatement = con.createStatement();
							String companyQuery = "select kkr_company_id from kkr_company where Company_ticker='"
									+ dividendKey.getString("symbol") + "'";
							ResultSet rsCompany = companyStatement.executeQuery(companyQuery);
							rsCompany.next();
							// System.out.println("j: "+j+" " +"company ID:
							// "+rsCompany.getInt(1));
							Integer companyId = rsCompany.getInt(1);
							Map<String, String> dividendData = new HashMap<String, String>();
							dividendData.put("company_id", companyId.toString());
							dividendData.put("company_name", dividendKey.getString("symbol"));
							if (dividendArr != null) {
								JSONObject dv = dividendArr.getJSONObject(0);

								String dd_date = dv.has("date") ? dv.getString("date") : null;
								String declared = dv.has("declared") ? dv.getString("declared") : null;
								String amount = dv.has("amount") ? dv.getString("amount") : "0.0";
								String payable = dv.has("payable") ? dv.getString("payable") : null;
								String divtype = dv.has("divtype") ? dv.getString("divtype") : "D";
								String frequency = dv.has("frequency") ? dv.getString("frequency") : "U";
								String currency = dv.has("currency") ? dv.getString("currency") : null;
								String divflag = dv.has("divflag") ? dv.getString("divflag") : null;
								String indicatedrate = dv.has("indicatedrate") ? dv.getString("indicatedrate") : null;
								String record = dv.has("record") ? dv.getString("record") : null;

								dividendData.put("date", dd_date);
								dividendData.put("declared", declared);
								dividendData.put("amount", amount);
								dividendData.put("payable", payable);
								dividendData.put("divtype", divtype);
								dividendData.put("frequency", frequency);
								dividendData.put("currency", currency);
								dividendData.put("divflag", divflag);
								dividendData.put("indicatedrate", indicatedrate);
								dividendData.put("record", record);

								/*
								 * Insert Data into two different database.
								 * kkrProd with connection con and kkrdev with
								 * conkkrDev
								 */
								InsertintoDividendFunds(con, dividendData, bw_dup, bw_insert,1);
								 InsertintoDividendFunds(conkkrDev,dividendData,bw_dup,bw_insert,0);
								// pSLocal.executeBatch();
								System.out.println("..................inerted................................");
							}
						}
					}
				}
			}
			bw_dup.close();
			bw_insert.close();
		} catch (Exception e) {
			System.out.println("the exception is : " + e);
		}

	}

	private static void InsertGroupNamesinList() {
		// TODO Auto-generated method stub
		groupName.add("DOW");
		groupName.add("DJHF");
		groupName.add("CBO");
		groupName.add("RUS");
		groupName.add("BATS");
		groupName.add("BZX");
		groupName.add("EDGX");
		groupName.add("NSD");
		groupName.add("NMF");
		groupName.add("OTC");
		groupName.add("OTO");
		groupName.add("NYE");
		groupName.add("AMX");

	}

	public static JSONObject getResultsFromQM(String exchangeGroup, String desired_date) throws IOException {
		// http://app.quotemedia.com/data/getDividendsByExchange.json?exgroup=NYE&webmasterId=102417&date=2018-04-12
		System.out.println("http://app.quotemedia.com/data/getDividendsByExchange.json?exgroup=" + exchangeGroup
				+ "&webmasterId=102417&date=" + desired_date + "");
		URL url = new URL("http://app.quotemedia.com/data/getDividendsByExchange.json?exgroup=" + exchangeGroup
				+ "&webmasterId=102417&date=" + desired_date + "");
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		String responseString = "";
		String enterpriseToken = "NWFiNjM2OGEtYjFmNy00YmNiLThlYTktOTQyMjM4ZGJjMGQ1";
		try {
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");
			conn.setRequestProperty("Authorization", "Bearer " + enterpriseToken);
			BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
			String output;
			StringBuffer response = new StringBuffer();
			while ((output = br.readLine()) != null) {
				response.append(output);
			}
			responseString = response.toString();
			br.close();
			JSONObject json = new JSONObject(responseString);
			return json;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public static void InsertintoDividendFunds(Connection con, Map<String, String> dividendData, BufferedWriter bw_dup,
			BufferedWriter bw_insert,int flag) throws SQLException, IOException {
		// TODO Auto-generated method stub
		Statement checkstatement = con.createStatement();

		DecimalFormat df = new DecimalFormat("#.#####");
		df.setRoundingMode(RoundingMode.CEILING);
		String converted_Amount = df.format(Double.parseDouble(dividendData.get("amount"))).toString();
		String SQL_QUERY = null;
		if (dividendData.get("record") == null) {
			SQL_QUERY = "SELECT * FROM zsenia_fund_dividends WHERE " + "kkr_company_id = '"
					+ dividendData.get("company_id") + "'" + "AND amount = '" + converted_Amount + "' "
					+ "and frequency = '" + dividendData.get("frequency") + "'and " + "zsenia_fund_dividends.date = '"
					+ dividendData.get("date") + "' and divtype='" + dividendData.get("divtype") + "'";
		} else {
			SQL_QUERY = "SELECT * FROM zsenia_fund_dividends WHERE " + "kkr_company_id = '"
					+ dividendData.get("company_id") + "' AND " + "record = '" + dividendData.get("record")
					+ "'AND amount = '" + converted_Amount + "' " + "and frequency = '" + dividendData.get("frequency")
					+ "'and " + "zsenia_fund_dividends.date = '" + dividendData.get("date") + "' and divtype='"
					+ dividendData.get("divtype") + "'";
		}

		ResultSet checkSet = checkstatement.executeQuery(SQL_QUERY);
		int size = 0;
		if (checkSet != null) {
			checkSet.beforeFirst();
			checkSet.last();
			size = checkSet.getRow();
		}

		if (size < 1) {
			System.out.println(SQL_QUERY);
			//PrintWriter output = new PrintWriter(new FileWriter(insertlog, true));
			
			PreparedStatement pSLocal = con.prepareStatement(
					"insert into zsenia_fund_dividends (kkr_company_id,company_ticker,amount,record,payable,divtype,declared,"
							+ "frequency,date,currency,divflag,indicatedrate) values (?,?,?,?,?,?,?,?,?,?,?,?)");

			pSLocal.setInt(1, Integer.parseInt(dividendData.get("company_id")));
			pSLocal.setString(2, dividendData.get("company_name"));
			pSLocal.setString(3, converted_Amount);
			pSLocal.setString(4, dividendData.get("record"));
			pSLocal.setString(5, dividendData.get("payable"));
			pSLocal.setString(6, dividendData.get("divtype"));
			pSLocal.setString(7, dividendData.get("declared"));
			pSLocal.setString(8, dividendData.get("frequency"));
			pSLocal.setString(9, dividendData.get("date"));
			pSLocal.setString(10, dividendData.get("currency"));
			pSLocal.setString(11, dividendData.get("divflag"));
			pSLocal.setString(12, dividendData.get("indicatedrate"));
			
			if(flag==1){
				bw_insert.write(dividendData.toString()+"\n");
			}
			

			pSLocal.execute();
		}
		else{
			if(flag==1){
				bw_dup.write(dividendData.toString()+"\n");
			}
			
		}

	}

}
