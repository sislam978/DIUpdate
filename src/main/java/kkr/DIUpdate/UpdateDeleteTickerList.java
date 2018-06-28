package kkr.DIUpdate;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import kkr.DIUpdate.Models.CompanyTickerInfo;
import kkr.DIUpdate.Models.kkr_company;

public class UpdateDeleteTickerList {
	private static String[] exGroupArr = { "DOW", "DJHF", "CBO", "RUS", "BATS", "BZX", "EDGX", "NSD", "NMF", "OTC",
			"OTO", "NYE", "AMX" };
	private static String iStatement = "INSERT INTO `validationdb`.`symbolslist`(`instrumenttype`, `longname`, `shortname`, `symbol`, `exchange`, `symbolstring`, `issuetype`,"
			+ " `sectype`, `isocfi`) VALUES (?,?,?,?,?,?,?,?,?)";

	public static Map<String, Map<Integer, String>> TypeWiseTcikerMapQM = new HashMap<String, Map<Integer, String>>();
	public static Map<String, Map<Integer, String>> TypeWiseTcikerMapDatBase = new HashMap<String, Map<Integer, String>>();
	public static Map<String, String> typeMap = new HashMap<String, String>();
	public static ArrayList<CompanyTickerInfo> tickerinfoQMList = new ArrayList<CompanyTickerInfo>();
	public static ArrayList<CompanyTickerInfo> addList = new ArrayList<CompanyTickerInfo>();
	public static ArrayList<String> deleteCompanyTickerList = new ArrayList<String>();
	public static Map<String, CompanyTickerInfo> symbolObjectMap = new HashMap<String, CompanyTickerInfo>();
	public static ArrayList<String> finalSymbolDeleteList = new ArrayList<String>();
	private static Connection connectLocal() throws ClassNotFoundException, SQLException {
		Class.forName("com.mysql.jdbc.Driver");
		Connection con = (Connection) DriverManager
				.getConnection("jdbc:mysql://localhost:3306/kkrdb?rewriteBatchedStatements=true", "root", "");
		con.setAutoCommit(true);
		return con;
	}
	
	public static Connection connectkkrDev() throws ClassNotFoundException, SQLException {
		Class.forName("com.mysql.jdbc.Driver");
		/*
		 * Connection string for kkrdev
		 * "jdbc:mysql://kkrdev.craeiofbogb9.us-west-2.rds.amazonaws.com:3306/kkrdb?rewriteBatchedStatements=true"
		 * ,"kkr_app","kkr123"
		 */
		Connection con = (Connection) DriverManager
				.getConnection("jdbc:mysql://kkrdev.craeiofbogb9.us-west-2.rds.amazonaws.com:3306/kkrdb?rewriteBatchedStatements=true","kkr_app","kkr123");
		con.setAutoCommit(true);
		return con;
	}
	

	public static void typeMapCreation(Connection con) throws SQLException {
		String sql = "SELECT DISTINCT Type FROM `kkr_company`";
		Statement st = con.createStatement();
		ResultSet rs = st.executeQuery(sql);
		while (rs.next()) {
			String typeString = rs.getString(1);
			typeMap.put(typeString.toLowerCase(), typeString.toLowerCase());
		}
	}

	public static void companyTickerMapTypeWise(Connection con) throws SQLException {
		for (Map.Entry<String, String> entry : typeMap.entrySet()) {
			String sql = "SELECT kkr_company_id,Company_ticker FROM `kkr_company` WHERE type ='" + entry.getKey() + "'";
			Statement st = con.createStatement();
			ResultSet rs = st.executeQuery(sql);
			Map<Integer, String> typeTcikerMap = new HashMap<Integer, String>();
			while (rs.next()) {
				Integer company_id = rs.getInt(1);
				String company_ticker = rs.getString(2);
				typeTcikerMap.put(company_id, company_ticker);
			}
			TypeWiseTcikerMapDatBase.put(entry.getKey(), typeTcikerMap);
		}
	}

	private static JSONObject getResultsFromQM(String exGroup) throws IOException {
		System.out.println("http://app.quotemedia.com/data/getSymbols.json?webmasterId=102417&exgroup=" + exGroup);
		URL url = new URL("http://app.quotemedia.com/data/getSymbols.json?webmasterId=102417&exgroup=" + exGroup);
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

	public static void createAllListnMaps(Connection cLocal) throws SQLException {
		// TODO Auto-generated method stub
		typeMapCreation(cLocal);
		companyTickerMapTypeWise(cLocal);
		int k = 0;
		for (String exGroup : exGroupArr) {
			System.out.println("Going to check for exGroup:" + exGroup);
			try {
				JSONObject jsonObject = getResultsFromQM(exGroup);
				JSONObject resultsJson = (JSONObject) jsonObject.get("results");
				JSONArray lookupdataJsonArr = resultsJson.has("lookupdata") ? resultsJson.getJSONArray("lookupdata")
						: null;
				if (lookupdataJsonArr != null) {
					for (int i = 0; i < lookupdataJsonArr.length(); i++) {
						JSONObject lookupDataJson = lookupdataJsonArr.getJSONObject(i);
						JSONObject equityinfoJSON = lookupDataJson.getJSONObject("equityinfo");
						JSONObject keyJSON = lookupDataJson.getJSONObject("key");

						String instrument_type = equityinfoJSON.has("instrumenttype")
								? equityinfoJSON.getString("instrumenttype") : null;
						String longname = equityinfoJSON.has("longname") ? equityinfoJSON.getString("longname") : null;
						String shortname = equityinfoJSON.has("shortname") ? equityinfoJSON.getString("shortname")
								: null;
						String symbol = keyJSON.has("symbol") ? keyJSON.getString("symbol") : null;
						String exchange = keyJSON.has("exchange") ? keyJSON.getString("exchange") : null;
						String symbolstring = lookupDataJson.has("symbolstring")
								? lookupDataJson.getString("symbolstring") : null;
						String issuetype = lookupDataJson.has("symbolstring") ? lookupDataJson.getString("symbolstring")
								: null;
						String sectype = equityinfoJSON.has("sectype") ? equityinfoJSON.getString("sectype") : null;
						String isocfi = equityinfoJSON.has("isocfi") ? equityinfoJSON.getString("isocfi") : null;
						// System.out.println("instrument_type: "
						// +instrument_type+":: longname: "+longname+"::
						// shortname: "+shortname+":: symbol: "
						// +symbol+":: exchange: "+symbolstring+":: issuetype:
						// "+issuetype+":: sectype: "+sectype+":: isocfi:
						// "+isocfi);

						// System.out.println("instrument: "+instrument_type);
						if (symbol.contains(":")|| symbol.equals(null)) {
							continue;
						}
						if (typeMap.containsKey(instrument_type.toLowerCase())) {
							CompanyTickerInfo cti = new CompanyTickerInfo();
							cti.setInstrumenttype(instrument_type);
							cti.setSymbol(symbol);
							cti.setLongname(longname);
							cti.setShorname(shortname);
							cti.setExchange(exchange);
							cti.setSymbol_string(symbolstring);
							cti.setIssuetype(issuetype);
							cti.setSectype(sectype);
							cti.setIsocfi(isocfi);
							if (!TypeWiseTcikerMapQM.containsKey(instrument_type)) {
								Map<Integer, String> mm = new HashMap<Integer, String>();
								mm.put(k, symbol);
								TypeWiseTcikerMapQM.put(instrument_type, mm);
								k++;
							} else {
								Map<Integer, String> mm = new HashMap<Integer, String>();
								mm.putAll(TypeWiseTcikerMapQM.get(instrument_type));
								mm.put(k, symbol);
								TypeWiseTcikerMapQM.put(instrument_type, mm);
								k++;
							}
							// tickerinfoQMList.add(cti);
							symbolObjectMap.put(symbol, cti);
							// System.out.println("Successful!!");
						}
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static void CreateAddDeleteList(Connection con) throws IOException {

	
		for (Map.Entry<String, Map<Integer, String>> eEntry : TypeWiseTcikerMapDatBase.entrySet()) {
			String instrumentType = eEntry.getKey();
			if (TypeWiseTcikerMapQM.containsKey(instrumentType)) {
				Map<Integer, String> mQM = new HashMap<Integer, String>();
				mQM.putAll(TypeWiseTcikerMapQM.get(instrumentType));
				int k=0;
				for (Map.Entry<Integer, String> eeEntry : eEntry.getValue().entrySet()) {
					String ticker = eeEntry.getValue();
					if (!mQM.containsValue(ticker)) {
						deleteCompanyTickerList.add(eeEntry.getValue());
						k++;
					}

				}
				System.out.println("type: " + instrumentType + " :: delete number size: " + k);
			}
		}
		BufferedWriter bw= new BufferedWriter(new FileWriter(new File("resources/deleteList.txt")));
		bw.write(deleteCompanyTickerList.toString());
		//bw.close();
		
		
		for (Map.Entry<String, Map<Integer, String>> eEntry : TypeWiseTcikerMapQM.entrySet()) {
			String instrumentType = eEntry.getKey();
			int m=0;
			if (TypeWiseTcikerMapDatBase.containsKey(instrumentType)) {
				Map<Integer, String> mDataBase = new HashMap<Integer, String>();
				mDataBase.putAll(TypeWiseTcikerMapDatBase.get(instrumentType));
				
				for (Map.Entry<Integer, String> eeEntry : eEntry.getValue().entrySet()) {
					String ticker = eeEntry.getValue();
					if (!mDataBase.containsValue(ticker)) {
						addList.add(symbolObjectMap.get(eeEntry.getValue()));
						m++;
					}
				}
				System.out.println("type: " + instrumentType + " :: add number: " + m);
			}
		}
		BufferedWriter bw1=new BufferedWriter(new FileWriter(new File("resources/addlist.txt")));
		
		for(int i=0;i<addList.size();i++){
			bw1.write(addList.get(i).toString()+"\n \n");
		}
		
		//bw.close();
		System.out.println("QM list: "+symbolObjectMap.size());
		System.out.println("Successfully Commpleted.");
	}

	public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {
		Connection cLocal = null;

		PreparedStatement pSLocal = null;
		cLocal = connectLocal();
		pSLocal = cLocal.prepareStatement(iStatement);

	//	PreparedStatement pSLocal = null;
		try {
			cLocal = connectkkrDev();
			//pSLocal = cLocal.prepareStatement(iStatement);
		} catch (ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			System.exit(1);
		}

		// TODO Auto-generated method stub
		createAllListnMaps(cLocal);
		CreateAddDeleteList(cLocal);
		
		Scanner input=new Scanner(System.in);
		String startDate=input.nextLine();
		String endDate=input.nextLine();
		for(int i=0;i<deleteCompanyTickerList.size();i++){
			GetFullPriceHistory gfph=new GetFullPriceHistory();
			try {
				boolean decisionFlag=gfph.finalDeleteList(deleteCompanyTickerList.get(i), startDate, endDate);
				if(decisionFlag) {
					finalSymbolDeleteList.add(deleteCompanyTickerList.get(i));
					
				}
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		BufferedWriter bw1=new BufferedWriter(new FileWriter(new File("resources/FinalDeletelist.txt")));
		
		for(int i=0;i<finalSymbolDeleteList.size();i++){
			bw1.write(finalSymbolDeleteList.get(i).toString()+"\n \n");
		}
		System.out.println("refined list: "+finalSymbolDeleteList.size());
	}

}
