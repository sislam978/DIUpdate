package kkr.DIUpdate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Scanner;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class GetFullPriceHistory {

	public static JSONObject getResultsFromQM(String ticker, String start_date, String end_date) throws IOException {
		URL url = new URL("http://app.quotemedia.com/data/getFullHistory.json?webmasterId=102417&symbol=" + ticker
				+ "&start=" + start_date + "&end=" + end_date);
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

	public static boolean finalDeleteList(String ticker, String start_date, String end_date)
			throws IOException, JSONException {

		String pClose = "";
		JSONObject resultsJSON = getResultsFromQM(ticker, start_date, end_date);
		if (resultsJSON != null) {
			JSONObject json1 = (JSONObject) resultsJSON.get("results");
			if (json1 != null) {
				JSONArray historyArr = json1.has("history") ? json1.getJSONArray("history") : null;
				if (historyArr != null) {
					JSONObject history = (JSONObject) historyArr.get(0);
					JSONArray eoddatas = history.has("eoddata") ? history.getJSONArray("eoddata") : null;
					// System.out.println("size of eoddatas:
					// "+eoddatas.length());
					if (eoddatas != null) {
						for (int i = 0; i < eoddatas.length(); i++) {
							JSONObject eoddata = eoddatas.getJSONObject(0);
							pClose = eoddata.getString("close");
							if (!pClose.equals(null)) {
								return false;
							}
						}
					}
				}
			}
		}
		return true;
	}

}
