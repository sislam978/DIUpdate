package kkr.DIUpdate.CommonUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils {
	
	public static String stringTodate(String date, String formatter, String format) throws Exception
	{
		//System.out.println(  "String   "+   date+ " formatter  "+ formatter+" format   "+ format);
		SimpleDateFormat desiredFormat = new SimpleDateFormat(format);
		SimpleDateFormat dateFormatter = new SimpleDateFormat(formatter); 

		Date newdate = null;
		String newDateString = null;
	    try {
	        newdate = dateFormatter.parse(date);
	        newDateString = desiredFormat.format(newdate);
	        //System.out.println(newDateString);
	    } catch (ParseException e) {
	        e.printStackTrace();
	        throw e;
	    }
		//System.out.println("newDateString : "+newDateString);
		return newDateString;
		
	}

}
