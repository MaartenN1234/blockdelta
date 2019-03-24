package mn.blockdelta.core;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import mn.blockdelta.core.conversions.BaseRow;

public class PageDataCollector {
	private final static int BLOCK_SIZE = 256 * 1024;
	private List<char []> collector;
	private char[]        current;
	private int           offset;
	
	public PageDataCollector(){
		collector = new ArrayList<char []>();
		current   = new char[BLOCK_SIZE];
		offset    = 0;		
	}
	
	private void testWrapBlock(){
		if (offset == BLOCK_SIZE){
			collector.add(current);
			current   = new char[BLOCK_SIZE];
			offset    = 0;
		}
	}
	private void collect(char value) {
		current[offset++] = value;
		testWrapBlock();
	}		
	public void collect(int value) {
		collect((char) (value >>> 16));
		collect((char) (value       ));
	}	

	public void collect(long value) {
		collect((char) (value >>> 48));
		collect((char) (value >>> 32));
		collect((char) (value >>> 16));
		collect((char) (value       ));
	}

	public void collect(double val) {
		long value = Double.doubleToLongBits(val);
		collect((char) (value >>> 48));
		collect((char) (value >>> 32));
		collect((char) (value >>> 16));
		collect((char) (value       ));
	}
	public void collect(String value) {
		if (value == null){
			collect((char) 0);
			return;
		}
		collect((char) (value.length()));
		for (int i = 0; i<value.length();i++)
			collect(value.charAt(i));
		
	}
	public void collect(Date value) {
		collect(value.getTime());
	}

	public PageData toPageData() {
		int pageSize   = collector.size()*BLOCK_SIZE +offset;
		char [] result = new char[pageSize];
		int position   = 0;
		for (char[] block : collector.toArray(new char[0][0])){
			System.arraycopy(block, 0, result, position, BLOCK_SIZE);
			position += BLOCK_SIZE;
		}
		System.arraycopy(current, 0, result, position, offset);
		
		return new PageData(result);
	}

	public void collect(BaseRow x) {
		x.collect(this);
	}

}
