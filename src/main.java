import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/***
 * 
 * @author benoitvillage
 * In this class, we will build bp object instanciation and compute avalability for each
 *
 */
public class main {

	/***
	 * 
	 * @param args : args(1) = date used for computation
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			if(args[0] != null)
				{
					//Validator.properties configuration initialisation 
					share_variable shareVariable = new share_variable();
					mysql_connection mycon = new mysql_connection(shareVariable);
					shareVariable.setMycon(mycon);
					
					mycon.readDataBase();
				
					DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

					share_variable.export_date = (String) args[0];
						
					group_bp my_group_bp = new group_bp();
					
					//log instanciation creation
					EON_log my_EON_log = new EON_log();
					
					//log instanciation creation
					EON_log_downtime my_EON_log_downtime = new EON_log_downtime();
					
					//mysql connection init
					
					
					//importation des bp_link
					business_process_link_importation(mycon);
					
					//importation des bp_serice
					services_importation(mycon);
					
					//importation des applications
					my_group_bp = createApplication(my_group_bp, mycon);
					
					//la récupération du state précédent pour chaque validateur
					//demande un accès sur les tables f_dtm_application_period 
					//et f_dtm_logs_nagios_period_downtime. Il faut dont créer
					//des index avant créationd des validators, puis les supprimer
					//avant injection dans ces tables.
					createIndexOnDWHTables(mycon);
					
					//pour chaque application il faut associé ces validateurs
					createApplicationValidator(my_group_bp);
					
					//on delete les index des tables pour ne pas encombrer l'injection
					deleteIndexOnDWHTables(mycon);
					
					
					createHighLevelApplicationLinkedCategory(my_group_bp);
					int i = 0;
					
					mycon.addchargement();
					
					/*//We increment end date of one day
					Calendar c_end = Calendar.getInstance();
					c_end.setTime(formatter.parse((String) args[1]));
					c_end.add(Calendar.DATE, i);*/
					
					//We initialise export date
					Calendar c = Calendar.getInstance();
					c.setTime(formatter.parse((String) args[0]));
					share_variable.export_date = formatter.format(c.getTime());
					
				//while (!share_variable.export_date.equals(formatter.format(c_end.getTime())))
				//{	
					System.out.println("Je calcule pour le jour " + share_variable.export_date);
					//Thread.sleep(3000);
					
					//application availability computation
					my_group_bp.compute_availability(my_EON_log, my_EON_log_downtime,mycon);
					
					
					i++;
					//increment current compute day
					/*c = Calendar.getInstance();
					c.setTime(formatter.parse((String) args[0]));
					c.add(Calendar.DATE, i);  // number of days to add
					share_variable.export_date = formatter.format(c.getTime());*/
				//}
					mycon.close();
					
			}
			else System.out.println ("usage : java - jar ETL_DTM_COMPUTE_STATE_JAR.jar YYYY_MM_DD");
		//	System.out.println("Veuillez donner la date d'importation en paramètre");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	private static void createIndexOnDWHTables(mysql_connection mycon) {
		// TODO Auto-generated method stub
		mycon.createIndexDWHTables();
		
	}

	private static void deleteIndexOnDWHTables(mysql_connection mycon) {
		// TODO Auto-generated method stub
		mycon.deleteIndexDWHTables();
		
	}

	/**
	 * his method create for each High Level BP an arrayList wich will contains
	 * state for each category linked to High Level BP.
	 * @param my_group_bp 
	 */
	private static void createHighLevelApplicationLinkedCategory(group_bp my_group_bp) {
		// TODO Auto-generated method stub
		
		for(int i = 0; i < share_variable.highLvlGroupBp.getGroup().size(); i++) {
			
			share_variable.highLvlGroupBp.getGroup().get(i).initializeCategoryList(my_group_bp);
			
		}
		
	}

	
	/**
	 * Application class creation from bp table
	 * @param my_group_bp 
	 * @param mycon 
	 * @return 
	 */
	private static group_bp createApplication(group_bp my_group_bp, mysql_connection mycon) {
		// TODO Auto-generated method stub
		my_group_bp = mycon.readApplications(my_group_bp);
		return my_group_bp;
	}
	
	/**
	 * importation of business_process_link
	 * @param mycon 
	 */
	private static void business_process_link_importation(mysql_connection mycon) {
		// TODO Auto-generated method stub
		mycon.process_link_import();
	}

	/**
	 * Importation of business_process_service
	 * @param mycon 
	 */
	private static void services_importation(mysql_connection mycon) {
		// TODO Auto-generated method stub
		mycon.service_import();
		
	}
	
	/**
	 * Create validator for each application
	 * @param my_group_bp 
	 */
	private static void createApplicationValidator(group_bp my_group_bp) {
		// TODO Auto-generated method stub
		my_group_bp.createValidators();
	}

}
