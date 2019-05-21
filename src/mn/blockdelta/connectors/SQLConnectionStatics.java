package mn.blockdelta.connectors;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import mn.blockdelta.config.ConfigFile;


public class SQLConnectionStatics {
	private static SQLConnectionStatics sqlStaticsStorage = new SQLConnectionStatics();
	private static int TIMEOUT    = ConfigFile.getSqlTimeOut();
	private static int FETCH_SIZE = ConfigFile.getSqlFetchSize();

	private String host    = ConfigFile.getSqlHost();
	private String port    = ConfigFile.getSqlPort();
	private String service = ConfigFile.getSqlService();
	private String user    = ConfigFile.getSqlUser();
	private String pass    = ConfigFile.getSqlPass();
	
	private Connection conn = null;

	static{
		DriverManager.setLoginTimeout(TIMEOUT);
	}

	private SQLConnectionStatics(){
	}

	// private object based methods start here
	private Connection getPoolConnectionS() throws SQLException{
		if (conn == null){
			conn = getConnectionS();
		}
		return conn;
	}
	private void setConnectionParametersS(String host, int port,	String service, String user, String pass) {
		this.host    = host;
		this.port    = ""+port;
		this.service = service;
		this.user    = user;
		this.pass    = pass;
		closePoolConnectionS();
	}
	private Connection getConnectionS() throws SQLException{
		Connection result;
		try {
			try {
				Class.forName ("oracle.jdbc.OracleDriver");
			} catch (ClassNotFoundException e1) {
				throw new SQLException("No oracle driver installed in javapath", e1);
			}
			result = DriverManager.getConnection
					("jdbc:oracle:thin:@//"+host+":"+port+"/"+service, user, pass);
			result.setAutoCommit (false);
		} catch (SQLException e){
			throw new SQLException("Connection failed. " + e.getMessage());
		}
		return result;
	}
	private void closePoolConnectionS(){
		if (conn != null){
			try {
				if(!conn.isClosed()){
					conn.rollback();
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			conn = null;
		}
	}

	// Public static methods start here
	public static Connection getPoolConnection() throws SQLException{
		return sqlStaticsStorage.getPoolConnectionS();
	}
	public static Connection getConnection() throws SQLException{
		return sqlStaticsStorage.getConnectionS();
	}
	public static void closePoolConnection(){
		sqlStaticsStorage.closePoolConnectionS();
	}
	public static void setConnectionParameters(String host, int port, String service, String user, String pass){
		sqlStaticsStorage.setConnectionParametersS(host, port, service, user, pass);
	}
	
	public static PreparedStatement prepareSQLSelect(String sql) throws SQLException{
		Connection conn = getPoolConnection();
		return conn.prepareStatement(sql);
	}
	
	public static void executeSQL(String sql) throws SQLException{
		PreparedStatement prepareStatement = prepareSQLSelect(sql);
		prepareStatement.execute();
		prepareStatement.close();

	}
	public static ResultSet getReadOnceResultSet(String sql) throws SQLException{
		ResultSet result = getPoolConnection()
								.createStatement()
								.executeQuery(sql);
		result.setFetchSize(FETCH_SIZE);
		return result;
	}


}
