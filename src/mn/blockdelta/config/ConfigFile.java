package mn.blockdelta.config;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class ConfigFile {
	private static final ConfigFile config = new ConfigFile();
	private Properties properties; 
	
	private ConfigFile(){
		properties = new Properties();
		try {
			properties.load(new FileInputStream("blockdelta.config"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	public static int getSqlTimeOut(){
		return Integer.parseInt(config.properties.getProperty("SQL_TIMEOUT")); 
	}
	public static int getSqlFetchSize(){
		return Integer.parseInt(config.properties.getProperty("SQL_FETCH_SIZE")); 
	}
	
	public static String getSqlHost(){
		return config.properties.getProperty("SQL_HOST"); 
	}
	public static String getSqlPort(){
		return config.properties.getProperty("SQL_PORT"); 
	}
	public static String getSqlService(){
		return config.properties.getProperty("SQL_SERVICE"); 
	}
	public static String getSqlUser(){
		return config.properties.getProperty("SQL_USER"); 
	}
	public static String getSqlPass(){
		return config.properties.getProperty("SQL_PASS"); 
	}
}
