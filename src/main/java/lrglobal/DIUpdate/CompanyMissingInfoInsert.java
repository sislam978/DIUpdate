package lrglobal.DIUpdate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class CompanyMissingInfoInsert {

	public static final Map<String, Integer> sectorMap = new HashMap<String, Integer>();

	private static Connection connectLocal() throws ClassNotFoundException, SQLException {
		Class.forName("com.mysql.jdbc.Driver");
		Connection con = (Connection) DriverManager
				.getConnection("jdbc:mysql://localhost:3306/kkrdb?rewriteBatchedStatements=true", "root", "");
		con.setAutoCommit(true);
		return con;
	}

	// private static Connection connectKKR() throws ClassNotFoundException,
	// SQLException {
	// Class.forName("com.mysql.jdbc.Driver");
	// //Connection con = (Connection)
	// DriverManager.getConnection("jdbc:mysql://kkrdev.craeiofbogb9.us-west-2.rds.amazonaws.com:3306/kkrdb?rewriteBatchedStatements=true","kkr_app","kkr123");
	// Connection con = (Connection)
	// DriverManager.getConnection("jdbc:mysql://kkrprod.craeiofbogb9.us-west-2.rds.amazonaws.com:3306/kkrdb?rewriteBatchedStatements=true","kkr_app","kkr123");
	// return con;
	// }

	private static JSONObject getResultsFromQMGCBS(String ticker) throws IOException {
		URL url = new URL("http://app.quotemedia.com/data/getCompanyBySymbol.json?webmasterId=102417&symbol="
				+ URLEncoder.encode(ticker, "UTF-8"));
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
			System.exit(1);
		}
		return null;
	}

	private static JSONObject getResultsFromQMGPFD(String ticker) throws IOException {
		URL url = new URL(
				"http://app.quotemedia.com/data/getPremiumFundData.json?webmasterId=102417&shareprice=true&symbol="
						+ ticker);
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
			JSONObject json = null;
			if (!responseString.equals(""))
				json = new JSONObject(responseString);
			return json;
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		return null;
	}

	public static void main(String[] args) {
		try {
			Connection cLocal = connectLocal();
			createSectorMap();
			Statement sKKR = cLocal.createStatement();
			// PreparedStatement pSLocal = cLocal.prepareStatement("INSERT INTO
			// `new_database`.`company_missing_data`
			// (`cid`,`ticker`,`ceo`,`sector`,`industry`,`short_description`,`long_description`,`instrument_type`)
			// VALUES (?,?,?,?,?,?,?,?)");
			ResultSet rSKKR = sKKR.executeQuery(
					"select company_ticker,kkr_company_id from kkr_company where company_ticker is not null "
							+ " and kkr_company_id not in (56009,64013,65750,76890,81408,169662, 169806, 169807,169810,169811, 169812,169813,169822,169853,170509,170675,172113,174471) and kkr_Company_id > 174466 order by kkr_company_id");
			int counter = 0;
			while (rSKKR.next()) {
				String ticker = rSKKR.getString(1);
				String cId = rSKKR.getString(2);
				String ceo = "";
				String sector = "";
				String industry = "";
				String shortdescription = "";
				String longdescription = "";
				String instrumenttype = "";

				counter++;

				System.out.println("running for ticker::" + ticker + "::company id::" + cId);

				JSONObject jOGCBS = getResultsFromQMGCBS(ticker);
				if (jOGCBS != null) {
					JSONObject jORGCBS = jOGCBS.has("results") ? jOGCBS.getJSONObject("results") : null;
					if (jORGCBS != null) {
						JSONArray jAGCBS = jORGCBS.has("company") ? jORGCBS.getJSONArray("company") : null;
						if (jAGCBS != null && jAGCBS.length() > 0) {
							JSONObject jOCGCBS = jAGCBS.getJSONObject(0);
							JSONObject jOPGCBS = jOCGCBS.has("profile") ? jOCGCBS.getJSONObject("profile") : null;
							if (jOPGCBS != null) {
								JSONObject jODGCBS = jOPGCBS.has("details") ? jOPGCBS.getJSONObject("details") : null;
								if (jODGCBS != null) {
									ceo = jODGCBS.has("ceo") ? jODGCBS.getString("ceo") : "";
								}

								JSONObject jOPCGCBS = jOPGCBS.has("classification")
										? jOPGCBS.getJSONObject("classification") : null;
								if (jOPCGCBS != null) {
									sector = jOPCGCBS.has("sector") ? jOPCGCBS.getString("sector") : "";
									industry = jOPCGCBS.has("industry") ? jOPCGCBS.getString("industry") : "";
								}

								shortdescription = jOPGCBS.has("shortdescription")
										? jOPGCBS.getString("shortdescription") : "";
								longdescription = jOPGCBS.has("longdescription") ? jOPGCBS.getString("longdescription")
										: "";
							}
						}
					}
				}
				/*
				 * else { System.out.
				 * println("null found in GCBS...........................");
				 * System.exit(1); }
				 */

				JSONObject jOGPFD = getResultsFromQMGPFD(ticker);
				if (jOGPFD != null) {
					JSONObject jORGPFD = jOGPFD.has("results") ? jOGPFD.getJSONObject("results") : null;
					if (jORGPFD != null) {
						JSONArray jAGPFD = jORGPFD.has("funds") ? jORGPFD.getJSONArray("funds") : null;
						if (jAGPFD != null && jAGPFD.length() > 0) {
							try {
								JSONObject jOFSGPFD = jAGPFD.getJSONObject(0);
								JSONArray jAFGPFD = jOFSGPFD.has("fund") ? jOFSGPFD.getJSONArray("fund") : null;
								if (jAFGPFD != null && jAFGPFD.length() > 0) {
									JSONObject jOFGPFD = jAFGPFD.getJSONObject(0);
									JSONObject jOSIGPFD = jOFGPFD.has("symbolinfo")
											? jOFGPFD.getJSONObject("symbolinfo") : null;
									if (jOSIGPFD != null) {
										JSONObject jOEIGPFD = jOSIGPFD.has("equityinfo")
												? jOSIGPFD.getJSONObject("equityinfo") : null;
										if (jOEIGPFD != null) {
											instrumenttype = jOEIGPFD.has("instrumenttype")
													? jOEIGPFD.getString("instrumenttype") : "";
										}
									}
								}
							} catch (Exception e) {
								// TODO Auto-generated catch block
								System.out.println("Fund data not found...");
								// e.printStackTrace();
							}
						}
					}
				}
				/*
				 * else { System.out.
				 * println("null found in jOGPFD...........................");
				 * System.exit(1); }
				 */
				String sql_query = null;
				String business_desc = shortdescription + " " +longdescription;
				String business_desFi=business_desc.replace("'", "\\'");
				System.out.println("Going to save the following for ticker::" + ticker + "::" + ceo + "::" + sector
						+ "::" + industry + "::" + shortdescription + "::" + longdescription + "::" + instrumenttype+ ":bb:" + business_desFi);
				// Statement psLocal=cLocal.createStatement();
				if (!instrumenttype.equals("")) {
					sql_query = "UPDATE kkrdb.kkr_company a SET a.type = '" + instrumenttype
							+ "' WHERE a.kkr_company_id = " + cId + "";
					Statement update_statement = cLocal.createStatement();
					update_statement.executeUpdate(sql_query);
				} if (!industry.equals("")) {
					sql_query = "UPDATE kkrdb.kkr_company a SET a.industry_type = '" + industry
							+ "' WHERE a.kkr_company_id = " + cId + "";
					Statement update_statement = cLocal.createStatement();
					update_statement.executeUpdate(sql_query);
				} if (!business_desFi.equals("")) {
					sql_query = "UPDATE kkrdb.kkr_company a SET a.business_description = '" + business_desFi
							+ "' WHERE a.kkr_company_id = " + cId + "";
					Statement update_statement = cLocal.createStatement();
					update_statement.executeUpdate(sql_query);
				} if (sectorMap.containsKey(sector)) {
					sql_query = "UPDATE kkrdb.kkr_company a SET a.sector_id = '" + sectorMap.get(sector)
							+ "' WHERE a.kkr_company_id = " + cId + "";
					Statement update_statement = cLocal.createStatement();
					update_statement.executeUpdate(sql_query);
				}

				/*
				 * kkr_company_management table data update
				 */
				if (!ceo.equals("")) {
					sql_query = "UPDATE kkrdb.kkr_company_management a SET a.manager_name = '" + ceo
							+ "' WHERE a.kkr_company_id = " + cId + "";
					Statement update_statement = cLocal.createStatement();
					update_statement.executeUpdate(sql_query);

					String insert_Query = "insert into kkrdb.kkr_company_management(kkr_company_id,company_position, manager_name, active) "
							+ "select kkr_company_id,'CEO','" + ceo
							+ "','Y' from kkr_company where kkr_company_id not in (select kkr_company_id from kkrdb.kkr_company_management)";
					Statement insert_statment = cLocal.createStatement();
					insert_statment.execute(insert_Query);
				}

			}

		} catch (ClassNotFoundException | SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void createSectorMap() {
		// TODO Auto-generated method stub
		sectorMap.put("Basic Materials", 502);
		sectorMap.put("Communication Services", 507);
		sectorMap.put("Consumer Cyclical", 503);
		sectorMap.put("Consumer Defensive", 506);
		sectorMap.put("Energy", 510);
		sectorMap.put("Financial Services", 501);
		sectorMap.put("Healthcare", 509);
		sectorMap.put("Industrials", 504);
		sectorMap.put("Real Estate", 158223);
		sectorMap.put("Technology", 505);
		sectorMap.put("Utilities", 508);

	}

}
