package mn.blockdelta.core;

import java.util.Date;

public class PageDataReader {
	private char [] data;
	private int     location;
	PageDataReader(PageData pageData){
		data     = pageData.data;
		location = 0;
	}
	private char readChar(){
		return data[location++];
	}
	public int readInt(){
		return  (int) (readChar()            << 16) +
				(int) (readChar() & 0xFFFF);
	}	
	
	public long readLong(){
		return  (long) ((readChar() & 0xFFFFL) << 48) +
			    (long) ((readChar() & 0xFFFFL) << 32) +
			    (long) ((readChar() & 0xFFFFL) << 16) +
			    (long) ((readChar() & 0xFFFFL));		
		
	}
	public double readDouble(){
		return Double.longBitsToDouble(readLong());
	}
	public String readString(){
		int    length = readChar();
		if (length == 0)
			return null;
		String result = new String(data, location, length);
		location +=length;
		return result;
	}
	public Date readDate(){
		return new Date(readLong());
	}
	public boolean hasNext(){
		return location < data.length;
	}
	
}
