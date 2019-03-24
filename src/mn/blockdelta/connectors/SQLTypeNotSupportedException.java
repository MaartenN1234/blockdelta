package mn.blockdelta.connectors;

public class SQLTypeNotSupportedException extends Exception {
	private int sqlColumnType;
	public SQLTypeNotSupportedException(int sqlColumnType) {
		this.sqlColumnType = sqlColumnType;
	}
	public String getMessage(){
		return "Column type " + sqlColumnType + " not supported.";
	}

}
