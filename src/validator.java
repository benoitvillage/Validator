import java.util.ArrayList;

/***
 * 
 * @author benoit
 * Validator validates a business process
 */
public class validator {

	/* validator state
	 * -1 : undetermined
	 * 0 : OK
	 * 2 : CRITICAL
	 * 3 : UNREACHABLE */
	private int state;
	
	/* validator state
	 * -1 : undetermined
	 * 0 : OK
	 * 2 : CRITICAL
	 * 3 : UNREACHABLE */
	private int previousState;
	
	/* validator type : services or bp */
	private String type;
	
	/* for type bp, id_1 = bp_name,
	 * for type services, id_1 = host_name */
	private String id_1 = "";
	
	/* for type bp, id_2 = null,
	 * for type services, id_2 = service_name */
	private String id_2 = "";
	
	/* For type bp, id_3 = bp source
	 * for type service, id_3 = host_service source */
	private String id_3 = "";
	

	/* State type is Host or Service. If critical is only host concerning, service==null and state is host.
	 * If service is not null */
	private String state_type;

	/* Gap is the difference between datetime log and code interval */
	private int gap;
	
	/* If and only if downtime, isDowntime = 1 else 0 */
	private int isDowntime;
	
	/* If and only if we found in downtime log a started type, we set this
	 * attribute to 1
	 */
	private int isDowntimeFlag;
	
	/* Previous value of downtime*/
	private int previousIsDowntime;

	/**LastState is used for keep the last state which occur during logs parsing*/
	private int lastState;

	private int gapDown;

	private int stateDown;
	
	private mysql_connection mycon;
	/**
	 * Constructor for bp type validator
	 * @param state
	 * @param type
	 * @param id
	 */
	public validator(int state, int previousState, String type, String bp_name, 
			String bp_source, mysql_connection mycon) {
		// TODO Auto-generated constructor stub
		this.state = state;
		this.previousState = previousState;
		this.lastState = previousState;
		this.isDowntime = -1;
		this.previousIsDowntime = -1;
		this.isDowntimeFlag = -1;
		this.type = type;
		this.id_1 = bp_name;
		this.id_3 = bp_source;
		this.mycon = mycon;
		
		//warning
		//System.out.println("Création d'un validateur de type bp " + bp_name);
		
	}
	
	/**
	 * Constructor for host-service validator
	 * @param state
	 * @param type
	 * @param id
	 */
	public validator(int state, String type, String host_name, String service_name, 
				String host_service_source, int previousState, int previousDowntime,
				mysql_connection mycon) {
		// TODO Auto-generated constructor stub
		this.state = state;
		this.previousState = previousState;
		this.lastState = previousState;
		this.type = type;
		this.id_1 = host_name;
		this.id_2 = service_name;
		this.id_3 = host_service_source;
		this.isDowntime = -1;
		this.previousIsDowntime = previousDowntime;
		this.isDowntimeFlag = -1;
		this.mycon = mycon;
		
		//warning
		//System.out.println("Création d'un validateur de type host_service " + host_name + " " + service_name + " " + type);
		
	}

	/**
	 * @return validator state
	 */
	public int getState() {
		// TODO Auto-generated method stub
		return this.state;
	}

	public String getType() {
		// TODO Auto-generated method stub
		return this.type;
	}

	/**
	 * @param my_EON_log = log unavailability for a five minute interval for given date
	 */
	public void computeState(EON_log my_EON_log, group_bp group_bp, String appli_name, String appli_source) {
		// TODO Auto-generated method stub
		
		if(this.type.equals("bp"))
		{
			
			this.test_application(group_bp, appli_name, appli_source);
			
		}
		else this.test_host_service(my_EON_log);
		
	}

	private void test_host_service(EON_log my_EON_log) {
		// TODO Auto-generated method stub
		ArrayList <ArrayList<String>> log_minutes = my_EON_log.getEon_log_table();	
		
		
		for(int i = 0; i < log_minutes.size(); i++)
		{
			if(log_minutes.get(i).get(3).equals(this.id_1) && log_minutes.get(i).get(1).equals(this.id_3) &&
					(log_minutes.get(i).get(4).equals(this.id_2) || log_minutes.get(i).get(4).equals("Hoststatus")))
			{
				//if log is in error state (2 or 3), we check current validator state is not already in error
				if((log_minutes.get(i).get(8).equals("2") || log_minutes.get(i).get(8).equals("3")) && 
						(this.state == -1  || this.state == 0 || this.state == 1) && !log_minutes.get(i).get(4).equals("Hoststatus")) {
				
					//warning
					//System.out.println("Nous trouvons en état " + log_minutes.get(i).get(8) + " dans le log à l'interval " + log_minutes.get(i).get(7) + " le host service " + this.id_1 + " " + this.id_2 );
					
					this.state = Integer.parseInt(log_minutes.get(i).get(8));
					
					if(!(this.previousState == 2 || this.previousState == 3)){
						this.gap = Integer.parseInt(log_minutes.get(i).get(11));
					
					}
					else this.gap = 0;
					
					this.state_type = log_minutes.get(i).get(10);
					this.lastState = Integer.parseInt(log_minutes.get(i).get(8));
				}
				else if((log_minutes.get(i).get(8).equals("2") || log_minutes.get(i).get(8).equals("3")) && 
						(this.state == 2  || this.state == 3) && !log_minutes.get(i).get(4).equals("Hoststatus")) {
				
					//warning
					//System.out.println("Nous trouvons en état " + log_minutes.get(i).get(8) + " dans le log à l'interval " + log_minutes.get(i).get(7) + " le host service " + this.id_1 + " " + this.id_2 );
					this.state_type = log_minutes.get(i).get(10);
					this.lastState = Integer.parseInt(log_minutes.get(i).get(8));
				}
				else if((log_minutes.get(i).get(8).equals("1") || log_minutes.get(i).get(8).equals("2")) && 
						(this.state == -1  || this.state == 0 || this.state == 1) && log_minutes.get(i).get(4).equals("Hoststatus")) {
				
					//S'il s'agit d'un outage host alors le state = 1. Nous affectons donc state = 2 pour unifier la signification state = 2
					//correspond à un CRITICAL, outage host-service ou seulement outage service
					this.state = 2;
					
					if(!(this.previousState == 2 || this.previousState == 3)){
						this.gap = Integer.parseInt(log_minutes.get(i).get(11));
					
					}
					else this.gap = 0;
					
					this.state_type = log_minutes.get(i).get(10);
					this.lastState = 2;
				}
				else if((log_minutes.get(i).get(8).equals("1") || log_minutes.get(i).get(8).equals("2")) && 
						(this.state == 2 || this.state == 3) && log_minutes.get(i).get(4).equals("Hoststatus")) {
				
					//S'il s'agit d'un outage host alors le state = 1. Nous affectons donc state = 2 pour unifier la signification state = 2
					//correspond à un CRITICAL, outage host-service ou seulement outage service
					
					this.lastState = this.state;
				}
				/*else if((log_minutes.get(i).get(8).equals("0") && log_minutes.get(i).get(4).equals("Hoststatus") 
						&& !this.id_2.equals("Hoststatus") && (this.state == 2 || this.state == 3) ))
				{
					//dans le cas ou un log correspondant au service a déjà été loggé, on ne prend pas les logs de type hostservice
				}*/
				else if (this.state != 2 && this.state != 3) {
					
					//warning
					//System.out.println("Nous trouvons en état " + log_minutes.get(i).get(8) + " dans le log à l'interval " + log_minutes.get(i).get(7) + " le host service " + this.id_1 + " " + this.id_2 );
					
					
					this.state = Integer.parseInt(log_minutes.get(i).get(8));

					this.state_type = log_minutes.get(i).get(10);
					
					if(this.previousState == 2 || this.previousState == 3){
						this.gap = Integer.parseInt(log_minutes.get(i).get(11));
					}
					else this.gap = 0;
					
					this.lastState = Integer.parseInt(log_minutes.get(i).get(8));
				}
				else if (this.state == 2 || this.state == 3) {
					
					this.gap = Integer.parseInt(log_minutes.get(i).get(12));
					this.lastState = Integer.parseInt(log_minutes.get(i).get(8));
				}
				
			}			
			//si host_log = validator_host et (service_log = validator_service ou service_log = null) et (state == 2 or state == 3)
			//validator_state = host_state
			//sinon validator_state = 0		
		}
		
	
		if (this.state== -1)
		{
			//warning
			//System.out.println("Aucune présence du host service " + this.id_1 + " " + this.id_2 + "dans le log pour l'interval " + log_minutes.get(0).get(7));
			
			if(this.previousState == 2 || this.previousState == 3){
				this.state = this.previousState;
			}
			else this.state=0;
			
			this.lastState = this.state;
			
			this.gap = 0;
			this.state_type = "no found";
			//this.isDowntime = this.previousIsDowntime;
		}
	}
	
	private void test_application(group_bp group_bp, String name, String source) {
		// TODO Auto-generated method stub
		
		this.state=group_bp.test_application_state(name, source);
		this.gap = group_bp.getApplicationGap(name, source);
		
	}
	
	/**
	 * This method dispatch bp class request depending on validator type 
	 * @param my_EON_log_downtime
	 * @param group_bp
	 * @param appli_name
	 * @param appli_source
	 */
	public void computeDowntime(EON_log_downtime my_EON_log_downtime, group_bp group_bp,
			String appli_name, String appli_source) {
		
		if(this.type.equals("bp"))
		{
			this.test_application_downtime(group_bp, appli_name, appli_source);
		}
		else this.test_host_service_downtime(my_EON_log_downtime);
		
	}
	
	/**
	 * This method test for all record of downtime log on code interval if
	 * this validator is present. In this case, isDowntime = 1
	 * @param my_EON_log_downtime
	 */
	private void test_host_service_downtime(EON_log_downtime my_EON_log_downtime) {
		// TODO Auto-generated method stub
		
		ArrayList <ArrayList<String>> log_minutes = my_EON_log_downtime.getEon_log_table();	
		
		for(int i = 0; i < log_minutes.size(); i++)
		{
			if(log_minutes.get(i).get(4).equals(this.id_1) && log_minutes.get(i).get(1).equals(this.id_3) &&
					(log_minutes.get(i).get(5).equals(this.id_2) || log_minutes.get(i).get(5).equals("Hoststatus")))
			{
				if (log_minutes.get(i).get(9).equals("STARTED")) {
				
					this.isDowntime = 1;
					this.isDowntimeFlag = 1;
					
				}
				else {
					this.isDowntime = 0;
				}
			}				
		}		
	
		//If validator has not been found in downtime log we keep previous downtime state
		if (this.isDowntime== -1 && this.previousIsDowntime != -1)
		{
			this.isDowntime = this.previousIsDowntime;
			if(this.isDowntime == 1)
			{
				this.isDowntimeFlag = 1;
			}
		}
		else if(this.isDowntime == -1)
		{
			this.isDowntime = 0;
			this.isDowntimeFlag = 0;
		}
		
		//determine gap_down;
		if(this.isDowntime == 0 && this.previousIsDowntime == 1) {
			this.gapDown = 0;
			this.stateDown = 0;
		}
		else if(this.isDowntime == 0)
		{
			this.gapDown = this.gap;
			this.stateDown = this.state;
		}
		else { 
			this.stateDown = 0;
			//if(this.state == 1)
			//	this.gapDown = this.gap;
			// else 
			this.gapDown = 0;
		}
		
		//warning
		//System.out.println("Le downtime du validator " + this.id_1 + " " + this.id_2 + " est à l'état " + this.isDowntime);
		
	}

	/**
	 * This method will do a request to group_bp class in order to test downtime state of the application
	 * @param group_bp
	 * @param appli_name
	 * @param appli_source
	 */
	private void test_application_downtime(group_bp group_bp,
			String appli_name, String appli_source) {
		this.isDowntime=group_bp.test_application_downtime(this.id_1, this.id_3);
		this.isDowntimeFlag=group_bp.test_application_downtime(this.id_1, this.id_3);
		this.computeStateDowntime();
		if(this.isDowntime == 1) this.gapDown = 0; else this.gapDown = this.gap;
		
	}

	private void computeStateDowntime() {
		// TODO Auto-generated method stub
		if(this.isDowntime==0)
		{
			this.stateDown = this.state;
		}
		else this.stateDown=1;
	}

	/**
	 * This method calls mysql_connection class insert_fact_dtm_logs method 
	 */
	public void insert_fact_dtm_logs(String application_name, String category, int code_interval) {
		// TODO Auto-generated method stub
		//warning
		if(this.type.equals("services")){
			mycon.insert_fact_dtm_logs(this.id_3,this.id_1,this.id_2,this.state_type, application_name, category, this.state, this.isDowntime, this.gap, code_interval, this.gapDown, this.stateDown);
		}
		/*if(application_name.equals("SAP_core_infra") && code_interval == 715)
		System.out.println("HOST " + this.id_1 + " : J'ai un downtime égal à " + this.isDowntime);}*/
	}

	/**
	 * 
	 * @param int
	 */
	public void setState(int b) {
		// TODO Auto-generated method stub
		this.state = b;
	}

	/**
	 * This method reinit validator state and save current state into previousState
	 */
	public void reinit() {

		this.state = -1;
		this.previousState = this.lastState;
		this.previousIsDowntime = this.isDowntime;
		this.isDowntime =  -1;
		this.isDowntimeFlag = -1;
		
	}

	/**
	 * 
	 * @return this.gap
	 */
	public int getIsDowntimeFlag() {
		// TODO Auto-generated method stub
		return this.isDowntimeFlag;
	}

	/**
	 * @return this.gap
	 */
	public int getGap() {
		// TODO Auto-generated method stub
		return this.gap;
	}

	public String getId1() {
		// TODO Auto-generated method stub
		return this.id_1;
	}

	public String getId2() {
		// TODO Auto-generated method stub
		return this.id_2;
	}

	public String getId3() {
		// TODO Auto-generated method stub
		return this.id_3;
	}

	/**
	 * 
	 * @param my_group_bp
	 * @return validator bp category
	 */
	public String getCategory(group_bp my_group_bp) {
		// TODO Auto-generated method stub
		return my_group_bp.getBpCategory(this.id_1, this.id_3);
	}

	public int getIsDowntime() {
		// TODO Auto-generated method stub
		return this.isDowntime;
	}

	public int getGapDown() {
		// TODO Auto-generated method stub
		return this.gapDown;
	}

	public int getStateDown() {
		// TODO Auto-generated method stub
		return this.stateDown;
	}

	public int getPreviousIsDowntimeFlag() {
		// TODO Auto-generated method stub
		return this.previousIsDowntime;
	}





	
}
