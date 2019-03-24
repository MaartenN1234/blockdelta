package mn.blockdelta.core.conversions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class RowsourceHeader {
	public static final int INTEGER_TYPE = 1;
	public static final int LONG_TYPE    = 2;
	public static final int DOUBLE_TYPE  = 3;
	public static final int STRING_TYPE  = 10;
	public static final int DATETIME_TYPE= 11;

	private final String [] columnNames;
	private final int    [] columnTypes; 
	private final Map<String, Integer> columnLookupMap;
	
	private final String rowSourceName;
	
	public RowsourceHeader (String rowSourceName,
							String [] columnNames,
							int    [] columnTypes){
		this.rowSourceName   = rowSourceName;
		this.columnNames     = columnNames;
		this.columnTypes     = columnTypes;
		this.columnLookupMap = createLookupMap(columnNames);
	}
	
	private Map<String, Integer> createLookupMap(String [] columnNames){
		Map<String, Integer> result = new HashMap<String, Integer>(columnNames.length);
		int i = 0;
		
		for (String columnName : columnNames)
			result.put(columnName, i++);
		
		return result;
	}
	
	public String getColumnName(int i){
		return columnNames[i];
	}
	public int getColumnType(int i){
		return columnTypes[i];
	}
	public String[] getColumnNames(){
		return columnNames;
	}
	public int[] getColumnTypes(){
		return columnTypes;
	}
	public int getColumnId(String s){
		Integer i = columnLookupMap.get(s);
		if (i==null){
			throw new RuntimeException("Column "+s+" not found in rowsource "+ rowSourceName);
		}
		return i;
	}

	/**
	 * @return the rowSourceName
	 */
	public String getRowSourceName() {
		return rowSourceName;
	}
	
}
