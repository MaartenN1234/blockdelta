package mn.blockdelta.core.conversions;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import mn.blockdelta.connectors.SQLConnectionStatics;
import mn.blockdelta.connectors.SQLTypeConversion;
import mn.blockdelta.connectors.SQLTypeNotSupportedException;
import mn.blockdelta.core.PageAdmin;
import mn.blockdelta.core.PageData;
import mn.blockdelta.core.PageDataCollector;

public class SQLTableLoader implements RowsourceGenerator {
	private RowsourceHeader            rowsourceHeader;
	private Function<String[], String> selectSqlForKeys;

	public SQLTableLoader(String rowsourceName, Function<String[], String> whereClauseSqlForKey){
		this(k -> "SELECT * FROM " + rowsourceName + " WHERE " + whereClauseSqlForKey.apply(k));
	}
	
	public SQLTableLoader(Function<String[], String> selectSqlForKey){
		this.selectSqlForKeys = selectSqlForKey;
		rowsourceHeader       = createRowsourceHeader();
	}
	
	private RowsourceHeader createRowsourceHeader() {
		String dummySql  = "SELECT * FROM ("+selectSqlForKeys.apply(EMPTY_OUTPUT_KEYS) + ") WHERE 1=0";
		ResultSetMetaData metaData;
		int               columnCount;
		ResultSet         resultSet;
		
		String[] columnNames;
		int[]    columnTypes;
		String   tableName   = "<NONE>";
		
		try {		
			resultSet   = SQLConnectionStatics.getReadOnceResultSet(dummySql);
			metaData    = resultSet.getMetaData();
			columnCount = metaData.getColumnCount();
			
			columnNames = new String[columnCount];
			columnTypes = new int[columnCount];
			
			for (int i=0; i<columnNames.length; i++){
				tableName      = metaData.getTableName(0);				
				columnNames[i] = metaData.getColumnName(i+1);
				columnTypes[i] = SQLTypeConversion.mapSQLTypeToInternal(metaData.getColumnType(i+1));
			}
			
			resultSet.getStatement().close();
		} catch (SQLException e) {
			throw new RuntimeException("SQLTableLoader::createRowsourceHeader - Initiation of datasource failed.\r\n"+
										"SQL: "+dummySql+"\r\n" +
										"Error: "+ e.getErrorCode() + " " + e.getMessage(), e);
		}  catch (SQLTypeNotSupportedException e) {
			throw new RuntimeException("SQLTableLoader::createRowsourceHeader -  Column Type can't be mapped.\r\n"+
					"SQL: "+dummySql+"\r\n" +
					"Error: "+ e.getMessage(), e);
		}
		
		
		return new RowsourceHeader( "SQLTableLoader::" +tableName,
									columnNames,
									columnTypes);
	}


	@Override
	public PageData execute(String[] outputKeys, Map<String, PageData> inputPageData) {
		String sql = selectSqlForKeys.apply(outputKeys);
		PageData result;
		
		try {
			ResultSet resultSet = SQLConnectionStatics.getReadOnceResultSet(sql);

			result = loadFromResultSet(resultSet);
			
			resultSet.getStatement().close();
		} catch (SQLException e) {
			throw new RuntimeException("SQLTableLoader::execute - Data fetch failed.\r\n"+
										"SQL: "+sql+"\r\n" +
										"Error: "+ e.getErrorCode() + " " + e.getMessage());
		}
		return result;
	}
	private PageData loadFromResultSet(ResultSet resultSet) throws SQLException{
		PageDataCollector pageDataCollector = new PageDataCollector();
		
		while(resultSet.next()){
			int columnID = 1;
			for (int type : rowsourceHeader.getColumnTypes()){
				switch(type){
					case RowsourceHeader.INTEGER_TYPE:
						pageDataCollector.collect(resultSet.getInt(columnID));
						break;
					case RowsourceHeader.LONG_TYPE:
						pageDataCollector.collect(resultSet.getLong(columnID));
						break;
					case RowsourceHeader.DOUBLE_TYPE:
						pageDataCollector.collect(resultSet.getDouble(columnID));
						break;
					case RowsourceHeader.STRING_TYPE:
						pageDataCollector.collect(resultSet.getString(columnID));
						break;
					case RowsourceHeader.DATETIME_TYPE:		
						pageDataCollector.collect(resultSet.getDate(columnID));
						break;
				}
				columnID++;
			}
		}		
		return pageDataCollector.toPageData();
	}



	@Override
	public boolean usesExternalInput() {
		return true;
	}
	@Override
	public RowsourceHeader getRowsourceHeader() {
		return rowsourceHeader;
	}


	@Override
	public Set<String> getInputPageKeysActual(String[] outputKeys, Map<String, PageData> inputPageData) {
		return EMPTY_INPUT_SET;
	}


}
