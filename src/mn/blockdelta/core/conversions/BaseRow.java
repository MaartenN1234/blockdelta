package mn.blockdelta.core.conversions;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import mn.blockdelta.core.PageDataCollector;
import mn.blockdelta.core.PageDataReader;

public class BaseRow {
	final private Object[]        storage;
	final private RowsourceHeader header;
	
	public BaseRow(RowsourceHeader header, Object... storage){
		this.header  = header;
		this.storage = storage;
	}
	public Object get(String attribute){
		return storage[header.getColumnId(attribute)];		
	}
	public String toString(){
		return Arrays.toString(storage);
	}
	public void collect(PageDataCollector collector){
		int [] columnTypes = header.getColumnTypes();
		int loc = 0;
		for (int type : columnTypes){
			Object o = storage[loc++];
			switch(type){
			case RowsourceHeader.INTEGER_TYPE:
				collector.collect((Integer) o);
				break;
			case RowsourceHeader.LONG_TYPE:
				collector.collect((Long) o);
				break;
			case RowsourceHeader.DOUBLE_TYPE:
				collector.collect((Double) o);
				break;
			case RowsourceHeader.STRING_TYPE:
				collector.collect((String) o);
				break;
			case RowsourceHeader.DATETIME_TYPE:		
				collector.collect((Date) o);
				break;
			}					
		}
	}
	
	public static BaseRow readFromReader(RowsourceHeader rowsourceHeader, PageDataReader pr){
		Object [] storage = new Object[rowsourceHeader.getColumnTypes().length];
		int location      = 0; 
		for (int type : rowsourceHeader.getColumnTypes()){
			switch(type){
				case RowsourceHeader.INTEGER_TYPE:
					storage[location] = pr.readInt();
					break;
				case RowsourceHeader.LONG_TYPE:
					storage[location] = pr.readLong();
					break;
				case RowsourceHeader.DOUBLE_TYPE:
					storage[location] = pr.readDouble();
					break;
				case RowsourceHeader.STRING_TYPE:
					storage[location] = pr.readString();
					break;
				case RowsourceHeader.DATETIME_TYPE:		
					storage[location] = pr.readDate();
					break;
			}
			location++;
		}
		return new BaseRow(rowsourceHeader, storage);		
	}
}
