import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Properties;


public class share_variable {
public static  String export_date;

public static ArrayList <ArrayList<String>> bp_links = new ArrayList <ArrayList<String>>();
public static ArrayList <ArrayList<String>> bp_services = new ArrayList <ArrayList<String>>();
public static int born_from = 0;
public static int born_to = 288;
public static group_bp highLvlGroupBp = new group_bp();
public static String highApplicationCategory = "NA";
public static String highApplicationNameSup = "Keycopter_portal";
public static int chargementId;
public mysql_connection mycon;



public String host ;
public String user ;
public String password;


public share_variable() {
	
	this.init_share_variable();
}

public void init_share_variable() {
	
   
    //try {
    	
    	//Properties configFile = new Properties();
		//configFile.load(this.getClass().getClassLoader().getResourceAsStream("/validator.properties"));
		
		//this.setHost(configFile.getProperty("HOST"));
		//this.setUser(configFile.getProperty("USER"));
		//this.setPassword(configFile.getProperty("PASSWORD"));
		
		System.out.println(this.host);
		System.out.println(this.user);
		System.out.println(this.password);
			
	
    /*} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}*/


	
}

public mysql_connection getMycon() {
	return mycon;
}

public void setMycon(mysql_connection mycon) {
	this.mycon = mycon;
}

public String getHost() {
	return host;
}

public void setHost(String host) {
	this.host = host;
}

public String getUser() {
	return user;
}

public void setUser(String user) {
	this.user = user;
}

public String getPassword() {
	return password;
}

public void setPassword(String password) {
	this.password = password;
}
	
}
