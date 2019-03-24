package mn.blockdelta.connectors;

import java.sql.Types;

import mn.blockdelta.core.conversions.RowsourceHeader;

public class SQLTypeConversion {
	public static int mapSQLTypeToInternal(int sqlColumnType) throws SQLTypeNotSupportedException{
		switch (sqlColumnType){
			case Types.SMALLINT:
			case Types.INTEGER:
				return RowsourceHeader.INTEGER_TYPE;
			case Types.BIGINT:
				return RowsourceHeader.LONG_TYPE;
			case Types.DECIMAL:
			case Types.NUMERIC:
			case Types.FLOAT:
			case Types.REAL:
			case Types.DOUBLE:
				return RowsourceHeader.DOUBLE_TYPE;
			case Types.CHAR:
			case Types.VARCHAR:
			case Types.NCHAR:
			case Types.NVARCHAR:
				return RowsourceHeader.STRING_TYPE;
			case Types.TIME:
			case Types.DATE:
			case Types.TIMESTAMP:
				return RowsourceHeader.DATETIME_TYPE;
			
			default:
				throw new SQLTypeNotSupportedException(sqlColumnType);
		}
		
	}
}
