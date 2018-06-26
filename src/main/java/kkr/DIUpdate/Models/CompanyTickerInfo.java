package kkr.DIUpdate.Models;

public class CompanyTickerInfo {
	
	String instrumenttype;
	String longname;
	String shorname;
	String symbol;
	String symbol_string;
	String exchange;
	String issuetype;
	String sectype;
	String isocfi;
	public String getInstrumenttype() {
		return instrumenttype;
	}
	public void setInstrumenttype(String instrumenttype) {
		this.instrumenttype = instrumenttype;
	}
	public String getLongname() {
		return longname;
	}
	public void setLongname(String longname) {
		this.longname = longname;
	}
	public String getShorname() {
		return shorname;
	}
	public void setShorname(String shorname) {
		this.shorname = shorname;
	}
	public String getSymbol() {
		return symbol;
	}
	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}
	public String getSymbol_string() {
		return symbol_string;
	}
	public void setSymbol_string(String symbol_string) {
		this.symbol_string = symbol_string;
	}
	public String getExchange() {
		return exchange;
	}
	public void setExchange(String exchange) {
		this.exchange = exchange;
	}
	public String getIssuetype() {
		return issuetype;
	}
	public void setIssuetype(String issuetype) {
		this.issuetype = issuetype;
	}
	public String getSectype() {
		return sectype;
	}
	public void setSectype(String sectype) {
		this.sectype = sectype;
	}
	public String getIsocfi() {
		return isocfi;
	}
	public void setIsocfi(String isocfi) {
		this.isocfi = isocfi;
	}
	@Override
	public String toString() {
		return "CompanyTickerInfo [instrumenttype=" + instrumenttype + ", longname=" + longname + ", shorname="
				+ shorname + ", symbol=" + symbol + ", symbol_string=" + symbol_string + ", exchange=" + exchange
				+ ", issuetype=" + issuetype + ", sectype=" + sectype + ", isocfi=" + isocfi + "]";
	}
	
	

}
