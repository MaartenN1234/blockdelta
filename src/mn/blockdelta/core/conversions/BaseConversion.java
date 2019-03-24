package mn.blockdelta.core.conversions;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import mn.blockdelta.core.PageData;

public abstract class BaseConversion implements RowsourceGenerator {
	@Override
	public boolean usesExternalInput() {
		return false;
	}

	private   final static DateFormat  df    = new SimpleDateFormat("yyyyMMdd");
	protected final static String FIRST_DATE = "20170101";

	protected static Date parseDate(String s){
		try {
			return df.parse(s);
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}	
	protected static Object moveDate(Object object, int move) {
		if (object instanceof String)
			return moveDate((String) object, move);
		if (object instanceof Date)
			return moveDate((Date) object, move);		
		throw new ClassCastException(object.getClass() + " can't be interpretended as a date.");
	}
	protected static Date moveDate(Date date, int move) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.add(Calendar.DATE, move);
		return c.getTime();
	}	
	protected static String moveDate(String stringDate, int move){
		return df.format(moveDate(parseDate(stringDate),move));
	}
	
	
	
	
	
	protected static double safeDivide(Object o1, Object o2, double defaultValue) {
		if (o1 instanceof Double &&
			o2  instanceof Double){
			double d1 = (Double) o1;
			double d2 = (Double) o2;
			if (Math.abs(d2) > 1E-10)
				return d1 / d2;
		}
		return defaultValue;
	}
}
