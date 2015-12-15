import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

public class mysql_connection {

	  private Connection connect_eor_ods = null;
	  private Connection connect_eor_dwh = null;
	  private Statement statement = null;
	  private PreparedStatement preparedStatement = null;
	  private ResultSet resultSet = null;
	  private String odsDatabase = "vaneon_ods";
	  private String dwhDatabase = "vaneon_dwh";
	  private share_variable shareVariable;
	  
	  public mysql_connection(share_variable shareVariable) {
		  
		  this.shareVariable = shareVariable;
		  
	  }
	  
	  public void readDataBase() throws Exception {
	    try {
	      // this will load the MySQL driver, each DB has its own driver
	      Class.forName("com.mysql.jdbc.Driver");
	      // setup the connection with the DB.
	      connect_eor_dwh = DriverManager
	          .getConnection("jdbc:mysql://localhost:3306/vaneon_dwh?"
	              + "user=vanillabmrt&password=SaintThomas,2014");
	      
	      connect_eor_ods = DriverManager
		          .getConnection("jdbc:mysql://localhost:3306/vaneon_ods?"
			              + "user=vanillabmrt&password=SaintThomas,2014");

	      
	    } catch (Exception e) {
	      throw e;
	    }

	  }

	  
	  public group_bp readApplications(group_bp my_group_bp)
	  {
	      try {
			statement = this.connect_eor_ods.createStatement();
			// resultSet gets the result of the SQL query
			resultSet = statement.executeQuery("select * from " + this.odsDatabase + ".bp");
			
			while (resultSet.next()) {
			      // it is possible to get the columns via name
			      // also possible to get the columns via the column number
			      // which starts at 1
			      // e.g., resultSet.getSTring(2);
				  bp my_bp;
			      String name = resultSet.getString("name");
			      String source = resultSet.getString("source");
			      String description = resultSet.getString("description");
			      String category = resultSet.getString("category");
			      String priority = resultSet.getString("priority");
			  	  String type = resultSet.getString("type");
				  String command =  resultSet.getString("command");
				  String url =  resultSet.getString("url");
				  String min_value =  resultSet.getString("min_value");
			      
			      my_bp = new bp(name, source, description, category, priority, type, command, url, min_value, this);
			      my_group_bp.add_group(my_bp);
			      
			      //We create list of high level bp
			      if(category.equals(share_variable.highApplicationCategory)
			    		  || name.equals(share_variable.highApplicationNameSup)){
			      share_variable.highLvlGroupBp.add_group(my_bp);
			      
			      //warning
			     // System.out.println("Je crée une application high level " + name);
			      
			      }
			    }
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    finally{
	    	  return my_group_bp;
	    }
	      
	  }

	  // you need to close all three to make sure
	  public void close() {
	    try {  	
			resultSet.close();
			statement.close();
		    connect_eor_dwh.close();
		    connect_eor_ods.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	   
	  }
	  private void close(Closeable c) {
	    try {
	      if (c != null) {
	        c.close();
	      }
	    } catch (Exception e) {
	    // don't throw now as it might leave following closables in undefined state
	    }
	  }

	  /***
	   * 
	   * @param export_date
	   * This method import EON logs for export_date
	   * @param five_minutes_interval 
	   * @param my_EON_log 
	   * @return 
	   */
	public EON_log import_logs(String export_date, int minutes_interval, EON_log my_EON_log) {
		// TODO Auto-generated method stub
		
	      try {
	    	  
			preparedStatement = connect_eor_dwh
			      .prepareStatement("SELECT fln_id,fln_source,fln_unix_time,dho_name, " 
			+						"CASE WHEN dse_name is null then 'Hoststatus' else dse_name END as dse_name,"
			+						"fln_date, fln_time, fln_code_interval, fln_state, met_type_label, met_state_type_label,"
			+    		  			"DATE_FORMAT(fln_time, '%H')*60 + DATE_FORMAT(fln_time, '%i') - fln_code_interval as gap, "
			+    		  			"fln_code_interval + 5 - (DATE_FORMAT(fln_time, '%H')*60 + DATE_FORMAT(fln_time, '%i')) as gap_inverse "
			+			 			"FROM f_dwh_logs_nagios "
			+								   "inner join d_host on f_dwh_logs_nagios.fln_host = d_host.dho_id " 
      		+ 								   		"AND f_dwh_logs_nagios.fln_source = d_host.dho_source "
      		+ 								   "left join d_service on f_dwh_logs_nagios.fln_service = d_service.dse_id "
      		+								 	    "AND f_dwh_logs_nagios.fln_source = d_service.dse_source "
      		+ 								   "inner join d_message_type on f_dwh_logs_nagios.fln_message_type = d_message_type.met_id "
			+			 			"WHERE fln_date = ? and fln_code_interval= ?  ");
			preparedStatement.setString(1, export_date);
			preparedStatement.setInt(2, minutes_interval);
			resultSet = preparedStatement.executeQuery();
			
			while (resultSet.next()) {
			      // it is possible to get the columns via name
			      // also possible to get the columns via the column number
			      // which starts at 1
			      // e.g., resultSet.getSTring(2);
				  ArrayList<String> log_row = new ArrayList<String>();
				  
			      log_row.add(resultSet.getString("fln_id"));
			      log_row.add(resultSet.getString("fln_source"));
			      log_row.add(resultSet.getString("fln_unix_time"));
			      log_row.add(resultSet.getString("dho_name"));
			      log_row.add(resultSet.getString("dse_name"));
			      log_row.add(resultSet.getString("fln_date"));
			      log_row.add(resultSet.getString("fln_time"));
			      log_row.add(resultSet.getString("fln_code_interval"));
			      log_row.add(resultSet.getString("fln_state"));
			      log_row.add(resultSet.getString("met_type_label"));
			      log_row.add(resultSet.getString("met_state_type_label"));
			      log_row.add(resultSet.getString("gap"));
			      log_row.add(resultSet.getString("gap_inverse"));
			      
			      my_EON_log.add(log_row);
			      
			    }
			
			return my_EON_log;
		    
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	     
		
	}
	
	/**
	 * 
	 * @param export_date
	 * @param five_minutes_interval
	 * @param my_EON_log_downtime
	 * @return the downtime log for the specified export date
	 */
	public EON_log_downtime import_logs_downtimme(String export_date,
			int five_minutes_interval, EON_log_downtime my_EON_log_downtime) {
		
	 try {
    	  
		preparedStatement = connect_eor_dwh
		      .prepareStatement("SELECT fdo_id,fdo_source,fdo_unix_time,fdo_datetime,dho_name," 
		    		+  			"CASE WHEN dse_name is null then 'Hoststatus' else dse_name END as dse_name, " 
		    		+  			"fdo_state, met_state_type_label, fdo_code_interval, met_type_label "
		      		+ 			"FROM f_dwh_logs_nagios_downtime "
		      		+				"inner join d_host on f_dwh_logs_nagios_downtime.fdo_host_id = d_host.dho_id " 
		      		+ 						"AND f_dwh_logs_nagios_downtime.fdo_source = d_host.dho_source "
		      		+ 				"left join d_service on f_dwh_logs_nagios_downtime.fdo_service_id = d_service.dse_id "
		      		+						"AND f_dwh_logs_nagios_downtime.fdo_source = d_service.dse_source "
		      		+ 				"inner join d_message_type on f_dwh_logs_nagios_downtime.fdo_message_type = d_message_type.met_id "
		      		+ 			"WHERE date_format(fdo_datetime, '%Y-%m-%d') = ? and fdo_code_interval= ?  ");
		preparedStatement.setString(1, export_date);
		preparedStatement.setInt(2, five_minutes_interval);
		resultSet = preparedStatement.executeQuery();
			
			while (resultSet.next()) {
			      // it is possible to get the columns via name
			      // also possible to get the columns via the column number
			      // which starts at 1
			      // e.g., resultSet.getSTring(2);
				  ArrayList<String> log_downtime_row = new ArrayList<String>();
				  
				  log_downtime_row.add(resultSet.getString("fdo_id"));
				  log_downtime_row.add(resultSet.getString("fdo_source"));
				  log_downtime_row.add(resultSet.getString("fdo_unix_time"));
				  log_downtime_row.add(resultSet.getString("fdo_datetime"));
				  log_downtime_row.add(resultSet.getString("dho_name"));
				  log_downtime_row.add(resultSet.getString("dse_name"));		
			      log_downtime_row.add(resultSet.getString("fdo_state"));
			      log_downtime_row.add(resultSet.getString("met_state_type_label"));
			      log_downtime_row.add(resultSet.getString("fdo_code_interval"));
			      log_downtime_row.add(resultSet.getString("met_type_label"));
			
			      my_EON_log_downtime.add(log_downtime_row);
			      
			    }
			
			return my_EON_log_downtime;
		    
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
	}

	/**
	 * bp_links table importation into share variable bp_links
	 */
	public void process_link_import() {
		// TODO Auto-generated method stub
		 try {
				statement = connect_eor_ods.createStatement();
				// resultSet gets the result of the SQL query
				resultSet = statement.executeQuery("select * from " + this.odsDatabase + ".bp_links");
				
				while (resultSet.next()) {
				      
					  ArrayList<String> bp_link_row = new ArrayList<String>();
					  
					  bp_link_row.add(resultSet.getString("id"));
					  bp_link_row.add(resultSet.getString("bp_name"));
					  bp_link_row.add(resultSet.getString("bp_link"));
					  bp_link_row.add(resultSet.getString("bp_name_source"));
					  bp_link_row.add(resultSet.getString("bp_link_source"));
				     
				      share_variable.bp_links.add(bp_link_row);
				      
				    }
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}

	/**
	 * bp_service importation into share variable bp_service
	 */
	public void service_import() {
		// TODO Auto-generated method stub
		 try {
				statement = connect_eor_ods.createStatement();
				// resultSet gets the result of the SQL query
				resultSet = statement.executeQuery("select * from " + this.odsDatabase + ".bp_services");
				
				while (resultSet.next()) {
				      
					  ArrayList<String> bp_service_row = new ArrayList<String>();
				      
					  bp_service_row.add(resultSet.getString("host_service_id"));
					  bp_service_row.add(resultSet.getString("host_service_source"));
					  bp_service_row.add(resultSet.getString("bp_name"));
					  bp_service_row.add(resultSet.getString("bp_source"));
					  bp_service_row.add(resultSet.getString("host"));
					  bp_service_row.add(resultSet.getString("service"));
					    
				      share_variable.bp_services.add(bp_service_row);
				      
				    }
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
	

	/**
	 * @param name : application name
	 * @param source 
	 * @param interval_minute : identifie une intervale de 5 minutes dans une journée
	 * @param category 
	 * @param state : état de l'application sur l'intervalle de 5 minutes
	 * @param gap 
	 * @param isDowntime 
	 */
	public void insert_fact_availability_application_period(String appli_name,
			String source, int interval_minute, String category, int state, int isDowntime, int gap, int state_downtime, int gapDown) {
		// TODO Auto-generated method stub
			
		try {
			
			DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
			Date date;
			//date = formatter.parse(share_variable.export_date);
			
			
			
			preparedStatement = connect_eor_dwh.prepareStatement("insert into f_dtm_application_period " +
					"(FAP_SOURCE, FAP_CHARGEMENT, FAP_APP_ID, FAP_DATE, FAP_CODE_INTERVAL, FAP_CATEGORY, FAP_STATE, FAP_STATE_DOWNTIME, FAP_GAP, FAP_IS_DOWNTIME, FAP_CATEGORY_ANALYSIS, FAP_GAP_DOWN) " + 
					" values (?,?,?,?,?,?,?,?,?,?,?,?)");
			preparedStatement.setString(1, source);
			preparedStatement.setInt(2, share_variable.chargementId);
		    preparedStatement.setLong(3, this.getApplicationId(appli_name,source));
		    preparedStatement.setString(4, share_variable.export_date);
		    preparedStatement.setLong(5, interval_minute);
		    preparedStatement.setInt(6, this.getCategoryId(category));
		    preparedStatement.setInt(7, state);
		    preparedStatement.setInt(8, state_downtime);
		    preparedStatement.setInt(9, gap);
		    preparedStatement.setInt(10, isDowntime);
		    preparedStatement.setLong(11, this.getCategoryAnalysisId("Global"));
		    preparedStatement.setLong(12, gapDown);
		    preparedStatement.executeUpdate();
		    
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (missing_application e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	   
		
	}

	
	public void insert_fact_category_availability_application_period(
			String appliname, String source, int fiveMinutesInterval,
			String category, int state, int state_downtime, int isDowntime, int gap,
			String categoryAnalysis, int gapDown) {
		
		try {
					
				preparedStatement = connect_eor_dwh.prepareStatement("insert into f_dtm_application_period " +
						"(FAP_SOURCE, FAP_CHARGEMENT, FAP_APP_ID, FAP_DATE, FAP_CODE_INTERVAL, FAP_CATEGORY, FAP_STATE, FAP_STATE_DOWNTIME, FAP_GAP, FAP_IS_DOWNTIME, FAP_CATEGORY_ANALYSIS, FAP_GAP_DOWN) " + 
						" values (?,?,?,?,?,?,?,?,?,?,?,?)");
				preparedStatement.setString(1, source);
				preparedStatement.setInt(2, share_variable.chargementId);
			    preparedStatement.setLong(3, this.getApplicationId(appliname,source));
			    preparedStatement.setString(4, share_variable.export_date);
			    preparedStatement.setLong(5, fiveMinutesInterval);
			    preparedStatement.setInt(6, this.getCategoryId(category));
			    preparedStatement.setLong(7, state);
			    preparedStatement.setLong(8, state_downtime);
			    preparedStatement.setLong(9, gap);
			    preparedStatement.setLong(10, isDowntime);
			    preparedStatement.setLong(11, this.getCategoryAnalysisId(categoryAnalysis));
			    preparedStatement.setLong(12, gapDown);
			    preparedStatement.executeUpdate();
			    
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (missing_application e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	
			
		}


	/**
	 * 
	 * @param source
	 * @param host_name
	 * @param service_name
	 * @param state_type
	 * @param application_name
	 * @param category
	 * @param state
	 * @param isDowntime
	 * @param gap
	 */
	public void insert_fact_dtm_logs(String source, String host_name, String service_name,
			String state_type, String application_name, String category,
			int state, int isDowntime, int gap, int code_interval, int gapDown, int stateDown) {
		// TODO Auto-generated method stub
		
		try {
			
			//warning
			//System.out.println("Je vais effectuer une insertion dans la table dtm_log pour le host-service " + host_name + " " + service_name + " " + code_interval);
			//Thread.sleep(2000);
		
			//date = formatter.parse(share_variable.export_date);
			
			
			preparedStatement = connect_eor_dwh.prepareStatement("insert into f_dtm_logs_nagios_period_downtime " +
					"(FLH_SOURCE, FLH_CHARGEMENT, FLH_CODE_INTERVAL, FLH_DATE, FLH_HOST, FLH_SERVICE, FLH_STATE_TYPE, FLH_APPLICATION, FLH_CATEGORY, FLH_STATE, FLH_STATE_DOWN, FLH_IS_DOWNTIME, FLH_GAP, FLH_GAP_DOWN) " + 
					" values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			preparedStatement.setString(1, source);
			preparedStatement.setInt(2, share_variable.chargementId);
			preparedStatement.setLong(3, code_interval);
			preparedStatement.setString(4, share_variable.export_date);
			preparedStatement.setLong(5, this.getHostId(host_name,source));
			preparedStatement.setLong(6, this.getServiceId(service_name,source));
			preparedStatement.setString(7, state_type);
		    preparedStatement.setLong(8, this.getApplicationId(application_name,source));
			preparedStatement.setInt(9, this.getCategoryId(category));
		    preparedStatement.setLong(10, state);
		    preparedStatement.setLong(11, stateDown);
		    preparedStatement.setLong(12, isDowntime);
		    preparedStatement.setLong(13, gap);
		    preparedStatement.setLong(14, gapDown);
		    preparedStatement.executeUpdate();
		    
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (missing_application e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
	}

	private int getCategoryId(String category) {
		// TODO Auto-generated method stub
		int category_id = -1;
	    ResultSet resultSet = null;
	    PreparedStatement preparedStatement = null; 
		// resultSet gets the result of the SQL query
	    
		
	    try {
	    	preparedStatement = connect_eor_dwh.prepareStatement("Select cat_id from d_category where cat_label = ?");
	    	preparedStatement.setString(1, category);
		    resultSet = preparedStatement.executeQuery();
			
			while (resultSet.next()) {	
			      
				category_id = resultSet.getInt("cat_id");
  
			    }
			if(category_id == -1)
			{
				throw new missing_application("Category " + category + "has not been find in table d_category");
			}
			return category_id;
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		} catch (missing_application e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
		finally{
			try {
				if(!(resultSet == null))
					resultSet.close();
				if(!(preparedStatement == null))
				preparedStatement.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private long getCategoryAnalysisId(String categoryAnalysis) {
		// TODO Auto-generated method stub
		int category_id = -1;
	    ResultSet resultSet = null;
	    PreparedStatement preparedStatement = null; 
		// resultSet gets the result of the SQL query
	    
		
	    try {
	    	preparedStatement = connect_eor_dwh.prepareStatement("Select caa_id from d_category_analysis where caa_label = ?");
	    	preparedStatement.setString(1, categoryAnalysis);
		    resultSet = preparedStatement.executeQuery();
			
			while (resultSet.next()) {	
			      
				category_id = resultSet.getInt("caa_id");
  
			    }
			if(category_id == -1)
			{
				throw new missing_application("Category " + categoryAnalysis + "has not been find in table d_category_analysis");
			}
			return category_id;
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		} catch (missing_application e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
		finally{
			try {
				if(!(resultSet == null))
					resultSet.close();
				if(!(preparedStatement == null))
				preparedStatement.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private long getServiceId(String service_name, String source) {
		// TODO Auto-generated method stub
		
		int service_id = -1;
	    ResultSet resultSet = null;
	    PreparedStatement preparedStatement = null; 
		// resultSet gets the result of the SQL query
	    
		
	    try {
	    
		    	preparedStatement = connect_eor_dwh.prepareStatement("Select dse_id from d_service where dse_name = ? and dse_source = ?");
		    	preparedStatement.setString(1, service_name);
			    preparedStatement.setString(2, source);
			    resultSet = preparedStatement.executeQuery();
			    
				
				while (resultSet.next()) {	
				      
					service_id = resultSet.getInt("dse_id");
	  
				    }
				if(service_id == -1)
				{
				    System.out.println("Le service "+ service_name + " source " + source + " dont nous cherchons l'état dans le log ne se trouve");
					throw new missing_application("Le service " + service_name + " source " + source + "n'a pas été trouvée dans la table d_service");
				}

			
	    	return service_id;
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		} catch (missing_application e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
		finally{
			try {
				if(!(resultSet == null))
					resultSet.close();
				if(!(preparedStatement == null))
				preparedStatement.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private long getHostId(String host_name, String source) {
		// TODO Auto-generated method stub
		
		 ResultSet resultSet = null;
		 PreparedStatement preparedStatement = null;
		try {
			 
	 		int host_id = -1;
	 		preparedStatement = connect_eor_dwh.prepareStatement("Select dho_id from d_host where dho_name = ? and dho_source = ?");
			// resultSet gets the result of the SQL query
		    preparedStatement.setString(1, host_name);
		    preparedStatement.setString(2, source);
		    resultSet = preparedStatement.executeQuery();
			
			while (resultSet.next()) {	
			      
				host_id = resultSet.getInt("dho_id");
  
			    }
			if(host_id == -1)
			{
				System.out.println("le host " + host_name + " source " + source + " n'est pas présent dans la table d_host");
				throw new missing_application("L'application " + host_name + " source " + source + "n'a pas été trouvée dans la table d_application");
			}
			return host_id;
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		} catch (missing_application e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
		finally{
			try {
				if(!(resultSet == null))
					resultSet.close();
				if(!(preparedStatement == null))
				preparedStatement.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * 
	 * @param appli_name
	 * @param source
	 * @return application id as stored in d_application table
	 * @throws missing_application
	 */
	private long getApplicationId(String appli_name, String source) throws missing_application  {
		// TODO Auto-generated method stub
		 
		ResultSet resultSet = null;
	    PreparedStatement preparedStatement = null;
		try {
			 
		 		int application_id = -1;
		 		preparedStatement = connect_eor_dwh.prepareStatement("Select dap_id from d_application where dap_name = ? and dap_source = ?");
				// resultSet gets the result of the SQL query
			    preparedStatement.setString(1, appli_name);
			    preparedStatement.setString(2, source);
			    resultSet = preparedStatement.executeQuery();
				
				while (resultSet.next()) {	
				      
					application_id = resultSet.getInt("dap_id");
	  
				    }
				if(application_id == -1)
				{
					throw new missing_application("L'application " + appli_name + " source " + source + "n'a pas été trouvée dans la table d_application");
				}
				return application_id;
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return -1;
			}
			finally{
				try {
					if(!(resultSet == null))
						resultSet.close();
					if(!(preparedStatement == null))
					preparedStatement.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
	}

	/**
	 * 
	 * @param linked_host
	 * @param linked_services
	 * @param linked_source
	 * @param validatorPreviousState 
	 * @return validator previous state
	 */
	public ArrayList<Integer> getValidatorState(String linked_host,
			String linked_services, String linked_source, ArrayList<Integer> validatorPreviousState) {
		// TODO Auto-generated method stub
		long host_id = this.getHostId(linked_host, linked_source);
		long service_id = this.getServiceId(linked_services, linked_source);
		int previous_state = 0;
		int previous_is_downtime = 0;
		
		validatorPreviousState = this.getStateMaxCodeInterval(host_id, service_id, linked_source, validatorPreviousState);
		
		
		return validatorPreviousState;
	}

	/**
	 * 
	 * 
	 * @param host_id
	 * @param service_id
	 * @param source
	 * @param validatorPreviousState
	 * @return
	 */
	private ArrayList<Integer> getStateMaxCodeInterval(long host_id,
			long service_id, String source, ArrayList<Integer> validatorPreviousState) {
		// TODO Auto-generated method stub
		
		
		try {
			
			DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
			String previousExportDateString;
			Date previousExportDate;
			int decalage;
			
			Calendar c = Calendar.getInstance();
			c.setTime(formatter.parse(share_variable.export_date));
			if(share_variable.born_from > 0) decalage = 0;
			else decalage = -1;
			
			c.add(Calendar.DATE, decalage);  // number of days to add
			previousExportDateString = formatter.format(c.getTime());
			previousExportDate = formatter.parse(previousExportDateString);
			
			if(decalage == -1){
				preparedStatement = connect_eor_dwh.prepareStatement("SELECT MAX(FLH_CODE_INTERVAL) as MAX_CODE_INTERVAL, FLH_STATE, FLH_IS_DOWNTIME FROM f_dtm_logs_nagios_period_downtime " 
																		+	"WHERE FLH_SERVICE = ? AND FLH_HOST = ? AND FLH_SOURCE = ? AND FLH_DATE = ?");
	
				preparedStatement.setLong(1, service_id);
				preparedStatement.setLong(2, host_id);
				preparedStatement.setString(3, source);
				preparedStatement.setString(4, previousExportDateString);
				
				//System.out.println("Nous récupérons les données datant de " + previousExportDate);
				//Thread.sleep(1000);
			}
			else {
				preparedStatement = connect_eor_dwh.prepareStatement("SELECT FLH_STATE, FLH_IS_DOWNTIME FROM f_dtm_logs_nagios_period_downtime " 
																		+	"WHERE FLH_SERVICE = ? AND FLH_HOST = ? AND FLH_SOURCE = ? AND FLH_DATE = ? AND FLH_CODE_INTERVAL = ?");

				preparedStatement.setLong(1, service_id);
				preparedStatement.setLong(2, host_id);
				preparedStatement.setString(3, source);
				preparedStatement.setString(4, previousExportDateString);
				preparedStatement.setLong(5, (share_variable.born_from-1)*5);
				//System.out.println("Nous récupérons les données datant de " + previousExportDate);
				//Thread.sleep(1000);
			}
		    resultSet = preparedStatement.executeQuery();
		    
		    if(resultSet.getFetchSize() == 0)
		    {
		    	validatorPreviousState.add(-1);
		    	validatorPreviousState.add(-1);
		    }
		    else
		    {
			    while(resultSet.next())
			    {
			    	validatorPreviousState.add((resultSet.getInt("FLH_STATE")));
			    	validatorPreviousState.add((resultSet.getInt("FLH_IS_DOWNTIME")));
			    }
		    }
		    
		    return validatorPreviousState;
		    
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return validatorPreviousState;
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return validatorPreviousState;
		}
		
		
	}

	/**
	 * This method allow to add a row into d_chargement and get the row id to
	 * store chargementId as Static into static class share_variable
	 */
	public void addchargement() {
		// TODO Auto-generated method stub
	
		
		// Create an instance of SimpleDateFormat used for formatting 
		// the string representation of date (month/day/year)
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

		// Get the date today using Calendar object.
		Date today = Calendar.getInstance().getTime();        
		// Using DateFormat format method we can create a string 
		// representation of a date with the defined format.
		String reportDate = df.format(today);

		// Print what date is today!
		System.out.println("Report Date: " + reportDate);
		
		try {
			preparedStatement = connect_eor_dwh.prepareStatement("INSERT INTO d_chargement (chg_source, chg_etl_name, chg_date) values (?,?,?)");
			preparedStatement.setString(2, "ETL_DTM_COMPUTE_STATE_JAR");
			preparedStatement.setString(1, this.dwhDatabase);
			preparedStatement.setString(3, reportDate);
			preparedStatement.executeUpdate();
			
			
			preparedStatement = connect_eor_dwh.prepareStatement("SELECT max(chg_id) + 1 as chg_id from d_chargement");
			resultSet = preparedStatement.executeQuery();
			
			
			while(resultSet.next())
			{
				share_variable.chargementId =  resultSet.getInt("chg_id");
			}
			
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	    
	}


	public void createIndexDWHTables() {
		// TODO Auto-generated method stub
		try {
			preparedStatement = connect_eor_dwh.prepareStatement("CREATE INDEX flh_date_index ON f_dtm_logs_nagios_period_downtime (FLH_DATE) USING BTREE;");
			preparedStatement.executeUpdate();
			
			preparedStatement = connect_eor_dwh.prepareStatement("CREATE INDEX fap_date_index ON f_dtm_application_period (FAP_DATE) USING BTREE;");
			preparedStatement.executeUpdate();
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}


	public void deleteIndexDWHTables() {
		// TODO Auto-generated method stub
		
		try {
			
			preparedStatement = connect_eor_dwh.prepareStatement("drop index flh_date_index on f_dtm_logs_nagios_period_downtime;");
			preparedStatement.executeUpdate();	
			
			preparedStatement = connect_eor_dwh.prepareStatement("drop index fap_date_index on f_dtm_application_period;");
			preparedStatement.executeUpdate();

			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
	}


	

	

	
} 

