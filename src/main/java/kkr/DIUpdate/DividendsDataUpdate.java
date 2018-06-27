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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.mysql.fabric.xmlrpc.base.Array;

import kkr.DIUpdate.CommonUtils.DataBaseUtils;

public class DividendsDataUpdate {
	public static final ArrayList<String> groupName = new ArrayList<String>();

	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub

		Scanner input = new Scanner(System.in);
		System.out.println("Quote Media Query paramters start date:(yyyy-MM-dd) ");
		String start_date = input.nextLine();
		System.out.println("Quote Media Query paramters end date:(yyyy-MM-dd) ");
		String end_date = input.nextLine();

		Connection ConKkrProd = DataBaseUtils.connectLocal();
		Connection ConKkrDev = DataBaseUtils.connectLocal();

		ArrayList<Map<String, String>> mapList = ReadQMDatanCreateListMap(start_date, end_date, ConKkrProd);

		String perent_directory = "resources/";
		String d_date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
		String[] splitdate = d_date.split("-");
		String insert_fileName = "insert_Log" + splitdate[0] + splitdate[1] + splitdate[2];
		String duplicate_log = "duplicate_log" + splitdate[0] + splitdate[1] + splitdate[2];
		String file_path = perent_directory + insert_fileName;
		String duplicate_filePath = perent_directory + duplicate_log;
		File insertLog = new File(file_path);
		File dupLog = new File(duplicate_filePath);

		BufferedWriter bw_insert = new BufferedWriter(new FileWriter(insertLog));
		BufferedWriter bw_dup = new BufferedWriter(new FileWriter(dupLog));

		InsertintoDividendFunds(ConKkrProd, mapList, bw_dup, bw_insert, 1);
		InsertintoDividendFunds(ConKkrDev, mapList, bw_dup, bw_insert, 1);

		bw_dup.close();
		bw_insert.close();

	}

	private static JSONObject getResultsFromQMTicker(String ticker, String start_date, String end_date)
			throws IOException {
		System.out.println("http://app.quotemedia.com/data/getDividendsBySymbol.json?webmasterId=102417&symbol="
				+ ticker + "&start= "+start_date+ "&end= " +end_date);
		URL url = new URL("http://app.quotemedia.com/data/getDividendsBySymbol.json?webmasterId=102417&symbol=" + ticker
				+ "&start=" + start_date + "&end=" + end_date + "");
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

	public static ArrayList<Map<String, String>> ReadQMDatanCreateListMap(String start_date, String end_date,
			Connection cKKR) {
		ArrayList<Map<String, String>> mapList = new ArrayList<Map<String, String>>();
		try {
			Statement sKKR = cKKR.createStatement();

			ResultSet rSetKKR = sKKR.executeQuery(
					"select kkr_company_id,Company_ticker from kkr_company where company_ticker ='NBD' "
							+ "and kkr_company_id not in (56009,64013,65750,76890,81408)"
							+ " and type<>'Index' and type <> 'sector' order by kkr_company_id");

			while (rSetKKR.next()) {
				
				Integer companyId = rSetKKR.getInt(1);
				String ticker = rSetKKR.getString(2);
				System.out.println("running for companyID::" + companyId + "::" + ticker);

				JSONObject jsonObject = getResultsFromQMTicker(ticker.trim(), start_date, end_date);

				JSONObject json1 = (JSONObject) jsonObject.get("results");

				JSONArray dividends = json1.has("dividends") ? json1.getJSONArray("dividends") : null;

				if (dividends != null) {
					JSONObject dividend1 = dividends.getJSONObject(0);
					if (dividend1 != null) {
						JSONArray dividendArr = dividend1.has("dividend") ? dividend1.getJSONArray("dividend") : null;
						if (dividendArr != null) {
							for (int i = 0; i < dividendArr.length(); i++) {
								Map<String, String> dividendData = new HashMap<String, String>();
								JSONObject dividend = dividendArr.getJSONObject(i);
								dividendData.put("company_id", companyId.toString());
								dividendData.put("company_name", ticker);
								dividendData.put("amount",
										dividend.has("amount") ? dividend.getString("amount") : "0.0");
								dividendData.put("record",
										dividend.has("record") ? dividend.getString("record") : null);
								dividendData.put("payable",
										dividend.has("payable") ? dividend.getString("payable") : null);
								dividendData.put("divtype",
										dividend.has("divtype") ? dividend.getString("divtype") : "D");
								dividendData.put("declared",
										dividend.has("declared") ? dividend.getString("declared") : null);
								dividendData.put("frequency",
										dividend.has("frequency") ? dividend.getString("frequency") : "U");
								dividendData.put("date", dividend.has("date") ? dividend.getString("date") : null);
								dividendData.put("currency",
										dividend.has("currency") ? dividend.getString("currency") : "USD");
								dividendData.put("divflag",
										dividend.has("divflag") ? dividend.getString("divflag") : "UR");
								dividendData.put("indicatedrate",
										dividend.has("indicatedrate") ? dividend.getString("indicatedrate") : null);

								mapList.add(dividendData);
							}

							System.out.println("..................completed for the ticker "+ticker+ "................................");
						}
					}

				}
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println(e);
			e.printStackTrace();
		}
		return mapList;
	}

	public static void InsertintoDividendFunds(Connection con, ArrayList<Map<String, String>> mList,
			BufferedWriter bw_dup, BufferedWriter bw_insert, int flag) throws SQLException, IOException {
		// TODO Auto-generated method stub
		for (int i = 0; i < mList.size(); i++) {
			Statement checkstatement = con.createStatement();
			Map<String,String> dividendData=mList.get(i);
			DecimalFormat df = new DecimalFormat("#.#####");
			df.setRoundingMode(RoundingMode.CEILING);
			String converted_Amount = df.format(Double.parseDouble(dividendData.get("amount"))).toString();
			String SQL_QUERY = null;
			if (dividendData.get("record") == null) {
				SQL_QUERY = "SELECT * FROM zsenia_fund_dividends WHERE " + "kkr_company_id = '"
						+ dividendData.get("company_id") + "'" + "AND amount = '" + converted_Amount + "' "
						+ "and frequency = '" + dividendData.get("frequency") + "'and "
						+ "zsenia_fund_dividends.date = '" + dividendData.get("date") + "' and divtype='"
						+ dividendData.get("divtype") + "' and divflag='" + dividendData.get("divflag") + "'";
			} else {
				SQL_QUERY = "SELECT * FROM zsenia_fund_dividends WHERE " + "kkr_company_id = '"
						+ dividendData.get("company_id") + "' AND " + "record = '" + dividendData.get("record")
						+ "'AND amount = '" + converted_Amount + "' " + "and frequency = '"
						+ dividendData.get("frequency") + "'and " + "zsenia_fund_dividends.date = '"
						+ dividendData.get("date") + "' and divtype='" + dividendData.get("divtype") + "'and divflag='"
						+ dividendData.get("divflag") + "'";
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
				PreparedStatement pSLocal = con.prepareStatement(
						"insert into zsenia_fund_dividends (kkr_company_id,company_ticker,amount,record,payable,divtype,declared,"
								+ "frequency,date,currency,divflag,indicatedrate, created_on) values (?,?,?,?,?,?,?,?,?,?,?,?,NOW())");

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
				
				pSLocal.execute();
				
				if (flag == 1) {
					bw_insert.write(dividendData.toString() + "\n");
				}
				
			} else {
				if (flag == 1) {
					bw_dup.write(dividendData.toString() + "\n");
				}
			}
		}
	}
}
