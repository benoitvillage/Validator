import java.util.ArrayList;
import java.sql.Date;


public class bp {

	private String name;
	private String source;
	private String description;
	private String priority;
	/**AND | OR | MIN*/
	private String type;
	private String category;
	private String command;
	private String url;
	private String min_value;
	
	/* 0 = available, 
	 * 2 = unavailable, 
	 *-1 = undetermined for an interval */
	private int state;
	
	/* 1 if and only if downtime for current interval si true
	 */
	private int isDowntime;
	
	
	/* validator list */
	private ArrayList<validator> validator_list;
	
	/*  0 if at least one validator has an indeterminate state
	 */
	private boolean validator_state_test = false;
	
	/* 0 if state is 1, else gap = minimum validator gap value in failure validator list*/
	private int gap;
	
	/* This arrayList contains : distinct categories of linked bp type validator, state, stateDown and isDowntime */
	private ArrayList<ArrayList<String>> linkedValidatorCategories =  new ArrayList<ArrayList<String>>();
	
	/*True if and only if bp attributes are determined for the current five minutes interval */
	private boolean isDetermined = false;
	private int stateDown;
	private int gapDown;
	private int previousIsDowntime;
	
	private mysql_connection mycon;
	
	private ArrayList<ArrayList<String>> listCategoryPreviousIsDowntime = new ArrayList<ArrayList<String>>();
	
	/**
	 * BP Constructor
	 * @param name
	 * @param source
	 * @param description
	 * @param category
	 * @param priority
	 * @param type
	 * @param command
	 * @param url
	 * @param min_value
	 */
	public bp(String name, String source, String description, String category, String priority, String type, String command, String url, String min_value, mysql_connection mycon)
	{
		this.name = name;
		this.source = source;
		this.description = description;
		this.category = category;
		this.priority = priority;
		this.type = type;
		this.command = command;
		this.url = url;
		this.min_value = min_value;
		this.mycon = mycon;
		
		//state is undetermined for the moment
		this.state = -1;
		this.stateDown = -1;
		this.gap = 0;
		this.gapDown = 0;
		
		/* initiation de la liste de validator */
		validator_list = new ArrayList<validator>();
		
		this.isDowntime = -1;
		this.previousIsDowntime = -1;

		
	}

	/**
	 * We find linked bp into share variable pb link and 
	 * linked host-service into share variable bp_services
	 */
	public void createValidators() {
		// TODO Auto-generated method stub
		
		//first we look for linked bp into share_variable bp_links
		this.find_linked_bp();
		
		//we look for linked service into share_variable bp_services
		this.find_linked_host_services();
		
	}

	/**
	 * We find linked bp into shared variable bp_link
	 */
	private void find_linked_bp() {
		
		for(int i=0; i < share_variable.bp_links.size(); i++){
			
			if(share_variable.bp_links.get(i).get(1).equals(this.name) && share_variable.bp_links.get(i).get(3).equals(this.source)){
				
				//warning
				//System.out.println("J'associe à la BP " + this.name + " la BP " + share_variable.bp_links.get(i).get(1));
				
				String linked_bp = share_variable.bp_links.get(i).get(2);
				String linked_bp_source = share_variable.bp_links.get(i).get(4);
				validator my_validator = new validator(-1, -1,"bp",linked_bp, linked_bp_source, this.mycon);
				this.validator_list.add(my_validator);
			}
			
		}
	}

	/**
	 * We find linked host_services into shared variable bp_services
	 */
	private void find_linked_host_services() {
		
		for(int i=0; i < share_variable.bp_services.size(); i++){
			if(share_variable.bp_services.get(i).get(2).equals(this.name) && share_variable.bp_services.get(i).get(3).equals(this.source)){
				String linked_host = share_variable.bp_services.get(i).get(4);
				String linked_services = share_variable.bp_services.get(i).get(5);
				String linked_source = share_variable.bp_services.get(i).get(1);
				int previousState = -1;
				int previousIsDowntime = -1;
				
				//warning
				//System.out.println("J'associe à la bp " + this.name + " le host service " + linked_host + " " + linked_services);
				
				ArrayList<Integer> validatorPreviousState = this.getValidatorPreviousState(linked_host, linked_services, linked_source);
				previousState = validatorPreviousState.get(0);
				previousIsDowntime = validatorPreviousState.get(1);
				
				validator my_validator = new validator(-1,"services",linked_host, linked_services, linked_source, previousState, previousIsDowntime,this.mycon);
				this.validator_list.add(my_validator);
			}
		}
	}

	/**
	 * This method calls mycon getValidatorState method and test getValidatorState method return no null result
	 * @param linked_host
	 * @param linked_services
	 * @param linked_source
	 * @return
	 */
	private ArrayList<Integer> getValidatorPreviousState(String linked_host,
			String linked_services, String linked_source) {
		// TODO Auto-generated method stub
		ArrayList<Integer> validatorPreviousState = new ArrayList<Integer>();
		validatorPreviousState = this.mycon.getValidatorState(linked_host,linked_services,linked_source, validatorPreviousState);
		
		if(validatorPreviousState.size() == 0){
			validatorPreviousState.add(-1);
			validatorPreviousState.add(-1);
		}
		
		return validatorPreviousState;
		
	}

	/**
	 * 
	 * @return bp state
	 */
	public int getState() {
		// TODO Auto-generated method stub
		return this.state;
	}

	/**
	 * 
	 * @param my_EON_log : unavailability log for a five minutes interval of a day
	 * @param my_EON_log_downtime 
	 * @param group_bp 
	 * 
	 */
	public void compute(EON_log my_EON_log, EON_log_downtime my_EON_log_downtime, int interval_minute, group_bp group_bp) {
		
		this.validator_state_test = true;
		if(this.validator_list.size() > 0) {
			for(int i = 0; i < this.validator_list.size(); i++){
				if(this.validator_list.get(i).getState() == -1){
					if(!this.validator_list.get(i).getType().equals("bp")){
						
						//warning
						//System.out.println("Nous testons le validateur " + this.validator_list.get(i).getId1() + " " + this.validator_list.get(i).getId2());
						
						this.validator_list.get(i).computeState(my_EON_log, group_bp, this.name, this.source);
						//this.validator_list.get(i).computeDowntime(my_EON_log_downtime, group_bp, this.name, this.source);
					}
					else {
						this.validator_list.get(i).computeState(my_EON_log, group_bp, this.validator_list.get(i).getId1(), this.validator_list.get(i).getId3());	
					}
					
					if(this.validator_list.get(i).getState() == -1)
						//validator_state_test if at least one application type validator state is undetermined (=-1)
						//Int this cas, bp state stay undetermined and we don't test bp availability.
						validator_state_test = false;
					else this.validator_list.get(i).computeDowntime(my_EON_log_downtime, group_bp, this.name, this.source);
				}	
			}
		}
		
	}
	
	/**
	 * Request by group_bp when this.validator_state_test is true
	 * This method test bp availability based on all validator state and downtime state.
	 * @param five_minutes_interval 
	 * @param k 
	 * @param j 
	 * @param i 
	 */
	public void determineBpAttribute(int five_minutes_interval, int etape, int nb_application_terminee, int nb_application_total, int nbtours){
		
			this.isDetermined = true;
			
			if(this.validator_list.size() > 0) {
				
				this.validator_test(five_minutes_interval);
			
			}else {
					this.validator_state_test = true;
					this.gap = 0;
					this.gapDown = 0;
					this.isDowntime = 0;
					this.state = 1;
					this.stateDown = 1;
			}
			
			this.insert_fact_availability_application_period(five_minutes_interval);
			
			//System.out.println("L'application " + this.name + " a le state " + this.state );
			//System.out.println("Etape " + etape + "/" + 12*24 + " : application " + nb_application_terminee +"/" + nb_application_total + " tour " + nbtours);
			
	}


	/**
	 * Test all validator state regarding application type (AND, OR)
	 * @param five_minutes_interval 
	 */
	private void validator_test(int five_minutes_interval) {
		
		if(this.type.equals("ET")){
			this.validator_test_and(five_minutes_interval);
		}
		else if(this.type.equals("OU"))
			this.validator_test_or(five_minutes_interval);	
		else this.validator_text_min_value(five_minutes_interval);
	}

	/**
	 * Test all validators with AND rule
	 * @param five_minutes_interval 
	 */
	private void validator_test_and(int five_minutes_interval) {
	
		int nbDowntime = 0;
		int nbFailure = 0;
		//warning
		if(this.name.equals("vmpub20_BasePub") && five_minutes_interval == 1175)
		{
			System.out.println("Debug");
		}

		//warning
		//System.out.println("Nous testons une application de type ET : " + this.name);
		
		for(int i=0; i < this.validator_list.size(); i++){
			if(((this.validator_list.get(i).getState() == 2 || this.validator_list.get(i).getState() == 3) && this.validator_list.get(i).getType().equals("services"))
					|| (this.validator_list.get(i).getState() == 0 && this.validator_list.get(i).getType().equals("bp"))){
				this.state = 0;
				nbFailure++;
				
				if(this.validator_list.get(i).getIsDowntimeFlag() == 1 || 
						(this.validator_list.get(i).getPreviousIsDowntimeFlag() == 1 && 
						 	this.validator_list.get(i).getType().equals("services"))){
					nbDowntime++;
				}
			}
		
			this.validator_list.get(i).insert_fact_dtm_logs(this.name, this.category,five_minutes_interval);
		}
		
		
		if(this.state != 0)
			this.state = 1;
		//else if(this.name.equals("Orion")){System.out.println("Je suis l'application " + this.name  + " est outage");}
		/*We have to solution to determine downtime state
		 * If there are as many failure as downtime, failure are only due to downtime and isDowntime = 1
		 * If there are more failure than downtime, failure ae not only due to downtime and isDowntime = 2;*
		 * Else isDowntime = 0*/
		
		if(nbDowntime == nbFailure && nbDowntime > 0)
			this.isDowntime = 1;
		else if(nbDowntime < nbFailure && nbDowntime > 0)
			this.isDowntime = 2;
		else this.isDowntime = 0;
		
		this.stateDown = this.computeStateDown(this.state, this.isDowntime);
		
		this.gap = this.computeGap("normal");
		this.gapDown = this.computeGap("down");
		//Compute gap
		
		
	}

	/**
	 * @param stateTemp
	 * @param isDowntimeTemp
	 * @return
	 */
	private int computeStateDown(int stateTemp, int isDowntimeTemp) {
		// TODO Auto-generated method stub
	
		int stateDownTemp = stateTemp;
		if(stateTemp == 0)
		{
			if(isDowntimeTemp == 1)
				stateDownTemp = 1;
			else stateDownTemp = 0;
		}
		else stateDownTemp = 1;
		
		return stateDownTemp;
	}

	/**
	 * 
	 * @param typeCalcul : down ou normal
	 * @return
	 */
	private int computeGap(String typeCalcul) {
		// TODO Auto-generated method stub
		
		ArrayList<Integer> listGapValidatorUp = new ArrayList <Integer>();
		ArrayList<Integer> listGapValidatorDown = new ArrayList <Integer>();
		int gapCompute = 0;
		int gapTemp = 0;
		int stateTemp = 0;
		String type = "";
		
		for(int i=0; i < this.validator_list.size(); i++){
			
			type = this.validator_list.get(i).getType();
			
			if(!typeCalcul.equals("down")) {
				gapTemp = this.validator_list.get(i).getGap();
				stateTemp = this.validator_list.get(i).getState();
			}
			else {
				gapTemp = this.validator_list.get(i).getGapDown();
				stateTemp = this.validator_list.get(i).getStateDown();
			}
			
			if((stateTemp == 2 || stateTemp == 3) && type.equals("services"))
			{
				listGapValidatorDown.add(gapTemp);
			}
			else if(stateTemp == 0 && type.equals("bp"))
				listGapValidatorDown.add(gapTemp);
			else listGapValidatorUp.add(gapTemp);
		}
		
		if(!typeCalcul.equals("down")) {
			if(this.state == 0)
				gapCompute = this.computeGapDown(listGapValidatorUp,listGapValidatorDown);
			else if(this.state == 1)
				gapCompute = this.computeGapUp(listGapValidatorUp,listGapValidatorDown);
		}
		else
		{
			if(this.stateDown == 0)
				gapCompute = this.computeGapDown(listGapValidatorUp,listGapValidatorDown);
			else if(this.stateDown == 1 && this.isDowntime == 0 )
				if(this.previousIsDowntime == 1)
					gapCompute = 0;
				else gapCompute = this.gap;
			else if(this.stateDown == 1 && this.isDowntime == 1 )
				gapCompute = 0;
		}
		
		return gapCompute;
		
		
	}

	/**
	 * 
	 * @param listGapValidatorUp : list of up validator gap
	 * @param listGapValidatorDown : list of down validator gap
	 * @return
	 */
	private int computeGapUp(ArrayList<Integer> listGapValidatorUp,
			ArrayList<Integer> listGapValidatorDown) {
		// TODO Auto-generated method stub
		int gapTemp = 0;
		
		if(this.type.equals("ET"))
		{
			gapTemp = this.getBiggestGap(listGapValidatorUp);
		}
		else {
			int gap1 = this.getSmallestGap(listGapValidatorUp);
			int gap0 = this.getSmallestGap(listGapValidatorDown);
			gapTemp = this.getPositiveIntegerOrNull(gap1 - gap0);
		}
		
		return gapTemp;
	}

	/**
	 * 
	 * @param listGapValidatorUp : list of up validator gap
	 * @param listGapValidatorDown : list of down validator gap
	 * @return bp gap
	 */
	private int computeGapDown(ArrayList<Integer> listGapValidatorUp,
			ArrayList<Integer> listGapValidatorDown) {
		int gapTemp = 0;
		
		if(this.type.equals("ET")) {
			int gap0 = this.getSmallestGap(listGapValidatorDown);
			int gap1 = this.getBiggestGap(listGapValidatorUp);
			gapTemp = this.getPositiveIntegerOrNull(gap0 - gap1);
		}
		else
			gapTemp = this.getBiggestGap(listGapValidatorDown);
		
		
		return gapTemp;
	}

	/**
	 * @param listGap
	 * @return
	 */
	private int getSmallestGap(ArrayList<Integer> listGap) {
		// TODO Auto-generated method stub
		int result = 5;
		
		for(int i = 0; i < listGap.size(); i++)
		{
			if(listGap.get(i) < result)
				result = listGap.get(i);
		}
		
		if(result == 5)
			result = 0;

		return result;
	}

	/**
	 * 
	 * @param listGap
	 * @return
	 */
	private int getBiggestGap(ArrayList<Integer> listGap) {
		
		int result = 0;
		
		for(int i = 0; i < listGap.size(); i++)
		{
			if(listGap.get(i) > result)
				result = listGap.get(i);
		}

		return result;
	}

	/**
	 * 
	 * @param i
	 * @return
	 */
	private int getPositiveIntegerOrNull(int i) {
		// TODO Auto-generated method stub
		int result = 0;
		
		if(i < 0)
			result = 0;
		else result = i;

		return result;
	}

	/**
	 * Test all validators with OR rule
	 * @param five_minutes_interval 
	 */
	private void validator_test_or(int five_minutes_interval) {	
		
		int nbDowntime = 0;
		
		for(int i=0; i < this.validator_list.size(); i++) {
			if((this.validator_list.get(i).getState() == 0 && this.validator_list.get(i).getType()=="services")
				||	(this.validator_list.get(i).getState() == 1 && this.validator_list.get(i).getType()=="bp")){
				this.state = 1;

			}
			
			if(this.validator_list.get(i).getIsDowntime() == 1){
				nbDowntime++;
			}
			/*if(this.name.equals("SAP_core_infra") && five_minutes_interval == 715)
			{
				System.out.println("Le host " + this.validator_list.get(i).getId1() + " a son downtime égal à " + this.validator_list.get(i).getIsDowntimeFlag());
			}*/
			
			this.validator_list.get(i).insert_fact_dtm_logs(this.name, this.category, five_minutes_interval);
		}	
		
		if(this.state != 1){
			this.state = 0;
		}
		if(nbDowntime == this.validator_list.size())
			this.isDowntime = 1;
		else this.isDowntime = 0;
		
		this.stateDown = this.computeStateDown(this.state,this.isDowntime);
		
		this.gap = this.computeGap("normal");
		this.gapDown = this.computeGap("down");

		

		
		/**if(this.name.equals("SAP_core_infra") && five_minutes_interval == 715)
		{
			System.out.println("Application SAP_core_infra "+ five_minutes_interval + " : le nbDowntime = " + nbDowntime);
			System.out.println("Application SAP_core_infra "+ five_minutes_interval + " : nb validator = " + this.validator_list.size());
			System.out.println("Application SAP_core_infra "+ five_minutes_interval + " : est downtime = " + this.isDowntime);
			System.out.println("Application SAP_core_infra "+ five_minutes_interval + " : state = " + this.state);

		}*/
	}
	
	/**
	 * This method tests min_value type bp state
	 * @param five_minutes_interval 
	 */
	private void validator_text_min_value(int five_minutes_interval) {
		// TODO Auto-generated method stub
		
		int nbDowntime = 0;
		int nbFailure = 0;
		int gap = 5;
		
		for(int i=0; i < this.validator_list.size(); i++){
			if(((this.validator_list.get(i).getState() == 2 || this.validator_list.get(i).getState() == 3) && this.validator_list.get(i).getType().equals("services"))
					|| (this.validator_list.get(i).getState() == 0 && this.validator_list.get(i).getType().equals("bp"))){
				nbFailure++;
				
				if(this.validator_list.get(i).getIsDowntimeFlag() == 1){
					this.isDowntime = 1;
					nbDowntime++;
				}
				
				if (this.validator_list.get(i).getGap() < gap)
					gap = this.validator_list.get(i).getGap();
			}
			
			this.validator_list.get(i).insert_fact_dtm_logs(this.name, this.category,five_minutes_interval);
		}
		
		/*If number of validator which are not in failure >= minimum validator ok define in bp min value
		 * state = 1. Else state = 0;*/
		//warning
		//System.out.println(this.type);
		if(this.validator_list.size() - nbFailure >= Integer.parseInt(this.min_value)){
			this.state = 1;
			
			if(gap == 5)
				gap = 0;
			this.gap = gap;
		}
		else {
			this.state = 0;
			this.gap = gap;
		}
		
		/*We have to solution to determine downtime state
		 * If there are as many failure as downtime, failure are only due to downtime and isDowntime = 1
		 * If there are more failure than downtime, failure ae not only due to downtime and isDowntime = 2;*
		 * Else isDowntime = 0*/
		if(nbDowntime >= Integer.parseInt(this.min_value))
			this.isDowntime = 1;
		else if(nbDowntime < nbFailure && nbDowntime > 0)
			this.isDowntime = 2;
		else this.isDowntime = 0;	
		
		this.stateDown = this.computeStateDown(this.state, this.isDowntime);
		this.gapDown = this.gap;
		
	}
	
	/**
	 * This method call mysql_connection method for inserting availibility row into
	 * f_availability_application_period
	 */
	private void insert_fact_availability_application_period(int interval_minute) {
		
		this.mycon.insert_fact_availability_application_period(this.name,this.source, 
				 interval_minute, this.category, this.state, this.isDowntime, this.gap,this.stateDown, this.gapDown);
	}

	public String getName() {
		
		return this.name;
	}

	/**
	 * Reinit appli state and all validators states
	 */
	public void reinit() {
		
		this.isDetermined = false;
		this.state = -1;
		this.validator_state_test = false;
		this.stateDown = -1;
		this.gapDown = 0;
		this.gap = 0;
		this.previousIsDowntime = this.isDowntime;
		this.isDowntime = -1;
		for (int i=0; i < this.validator_list.size(); i++) {
			this.validator_list.get(i).reinit();
		}
		if(this.category.equals(share_variable.highApplicationCategory) || this.name.equals(share_variable.highApplicationNameSup))
		{
			
			//we reinit the states linked to the category
			for(int i=0; i < this.linkedValidatorCategories.size(); i++)
			{
				int size = this.linkedValidatorCategories.get(i).size()-1;
				
				for(int j=0; j < size ; j++ ) {
					this.linkedValidatorCategories.get(i).remove(1);
				}
						
			}
		}
	}

	/**
	 * This method returns bp source
	 * @return
	 */
	public String getSource() {
		// TODO Auto-generated method stub
		return this.source;
	}

	/**
	 * This method returns bp Downtime state
	 * @return
	 */
	public int getDowntime() {
		// TODO Auto-generated method stub
		return this.isDowntime;
	}
	
	/**
	 * This method returns bp validator test state
	 */
	public boolean getValidator_state_test() {
		// TODO Auto-generated method stub
		return this.validator_state_test;
	}

	/**
	 * This method initialize list of validator categories linked to this bp
	 * @param my_group_bp 
	 */
	public void initializeCategoryList(group_bp my_group_bp) {
		// TODO Auto-generated method stub
		
		String categoryString = "";
		
		for(int i = 0; i < this.validator_list.size(); i++)
		{
			ArrayList<String> category = new ArrayList<String>();
			
			if(this.validator_list.get(i).getType().equals("bp"))	
				categoryString = this.validator_list.get(i).getCategory(my_group_bp);
				if(!this.verifyPresenceCategory(categoryString)){
					category.add(this.validator_list.get(i).getCategory(my_group_bp));
					this.linkedValidatorCategories.add(category);
					
					//warning
					/*System.out.println("J'ajoute la catégorie " + categoryString + "à l'application high level " 
											+ this.name + " provenant de l'appli " + this.validator_list.get(i).getId1() + " source "
											+ this.validator_list.get(i).getId3());*/
				}
						
		}
		
	}
	
	/**
	 * 
	 * @param categoryString
	 */
	private boolean verifyPresenceCategory(String categoryString) {
		// TODO Auto-generated method stub
		
		boolean isPresent = false;
		
		for(int i = 0; i < this.linkedValidatorCategories.size(); i++) {
			
				if(this.linkedValidatorCategories.get(i).get(0).equals(categoryString))
					isPresent = true;
				
		}
		return isPresent;	
	}

	/**
	 * 
	 * @return this.category
	 */
	public String getCategory() {
		// TODO Auto-generated method stub
		//warning
		//System.out.println("BP " + this.name + " je retourne ma catégorie égale à " + this.category);
		
		return this.category;
	}

	/**
	 * This method determines for each highbp linked category, the state, the stateDown and the isDowntime attributes
	 * @param group_bp 
	 */
	public void determineListCategoryAttributes(group_bp group_bp) {
		// TODO Auto-generated method stub
		
		if(this.name.equals("Keycopter_portal"))
		{
			System.out.println("Je suis l' application " + this.name + " et je suis à l'état 0");
		}
		
		ArrayList<ArrayList<String>> listStateBpByCategory =  new ArrayList<ArrayList<String>>();
		
		listStateBpByCategory = this.determineCategoryAttribute(group_bp, listStateBpByCategory);
		this.determineHighBpState(listStateBpByCategory);
		
	}

	/**
	 * This method compute state bp state, stateDown and isDowntime for a category, based on
	 * arraylist set in parameter
	 * @param listStateBpByCategory
	 */
	private void determineHighBpState(
			ArrayList<ArrayList<String>> listStateBpByCategory) {
		// TODO Auto-generated method stub
		
		for(int i=0; i < this.linkedValidatorCategories.size(); i++)
		{
			if(this.type.equals("ET")){
				this.determineEtHighBPAttributes(listStateBpByCategory,this.linkedValidatorCategories.get(i).get(0), i);
			}
			else if(this.type.equals("OU")) {
				this.determineOuHighBPAttributes(listStateBpByCategory,this.linkedValidatorCategories.get(i).get(0), i);
			}
			else this.determineMinHighBPAttributes(listStateBpByCategory,this.linkedValidatorCategories.get(i).get(0), i);
		}
	}

	/**
	 * Compute state of ET High Level BP for a specific category
	 * @param listStateBpByCategory
	 * @param string 
	 * @param indiceBPList 
	 * @return 
	 */
	private void determineMinHighBPAttributes(
			ArrayList<ArrayList<String>> listStateBpByCategory, String string, int indiceBPList) {
		// TODO Auto-generated method stub
		
		int nbDowntime = 0;
		int nbFailure = 0;
		int gap = 5;
		int nbCategory = 0;
		int stateDown = 0;
		int state = 0;
		int isDowntime = 0;
		int gapDown = 0;
		
		
		//warning
		//System.out.println("Nous testons une application de type ET : " + this.name);
		//listStateBpByCategory.get(i).get(0) = category
		//listStateBpByCategory.get(i).get(1) = state
		//listStateBpByCategory.get(i).get(2) = isDowntime
		//listStateBpByCategory.get(i).get(3) = gap
		for(int i=0; i < listStateBpByCategory.size(); i++){
			
			if(listStateBpByCategory.get(i).get(0).equals(category)) {
				
				nbCategory++;
				
				if(listStateBpByCategory.get(i).get(1).equals("0")){
					nbFailure++;
					
					if(listStateBpByCategory.get(i).get(2).equals("1")){
						nbDowntime++;
					}
					
					if (Integer.parseInt(listStateBpByCategory.get(i).get(3)) < gap)
						gap = Integer.parseInt(listStateBpByCategory.get(i).get(3));
				}
			}
		}
		
		/*If number of validator which are not in failure >= minimum validator ok define in bp min value
		 * state = 1. Else state = 0;*/
		//warning
		//System.out.println(this.type);
		if(nbCategory - nbFailure >= Integer.parseInt(this.min_value)){
			state = 1;
			if(gap == 5)
				gap = 0;
		}
		else {
			state = 0;
		}
		
		/*We have to solution to determine downtime state
		 * If there are as many failure as downtime, failure are only due to downtime and isDowntime = 1
		 * If there are more failure than downtime, failure ae not only due to downtime and isDowntime = 2;*
		 * Else isDowntime = 0*/
		if(nbDowntime >= Integer.parseInt(this.min_value))
			isDowntime = 1;
		else if(nbDowntime < nbFailure && nbDowntime > 0)
			isDowntime = 2;
		else isDowntime = 0;	
		
		stateDown = this.computeStateDown(state, isDowntime);
		gapDown = gap;
		
		//we build the state list for the category
		this.linkedValidatorCategories.get(indiceBPList).add(""+state);
		this.linkedValidatorCategories.get(indiceBPList).add(""+stateDown);
		this.linkedValidatorCategories.get(indiceBPList).add(""+isDowntime);	
		this.linkedValidatorCategories.get(indiceBPList).add(""+gap);
		this.linkedValidatorCategories.get(indiceBPList).add(""+gapDown);
		
	}

	/**
	 * Compute state of OU High Level BP for a specific category
	 * @param listStateBpByCategory
	 * @param indiceBPList 
	 * @param string 
	 * @return 
	 */
	private void determineOuHighBPAttributes(
			ArrayList<ArrayList<String>> listStateBpByCategory, String category, int indiceBPList) {
		// TODO Auto-generated method stub
		
		int nbDowntime = 0;
		int gap = 0;
		int gapDown = 0;
		int state = 0;
		int isDowntime = 0;
		int stateDown = 0;
		int nbValidatorCategory = 0;

		for(int i=0; i < listStateBpByCategory.size(); i++) {
			
			if(listStateBpByCategory.get(i).get(0).equals(category))
			{
				nbValidatorCategory++;
				if(listStateBpByCategory.get(i).get(1).equals("1")){
					state = 1;
				}
				else {	
					if(listStateBpByCategory.get(i).get(2).equals("1")){
						nbDowntime++;
					}
				}			
			}
		}	
		
		if(state != 1){
			state = 0;
		}
		if(nbDowntime == nbValidatorCategory)
			isDowntime = 1;
		isDowntime = 0;
		
		//init
		if(this.listCategoryPreviousIsDowntime.size() == 0)
			this.replaceCurrentPreviousDowntimeCategory(category,isDowntime);
		
		stateDown = this.computeStateDown(state, isDowntime);
		gap = this.computGapHighAppli(listStateBpByCategory, category,state,stateDown, isDowntime, gap, "normal");
		gapDown = this.computGapHighAppli(listStateBpByCategory, category, state, stateDown, isDowntime, gap, "down");
		
		this.replaceCurrentPreviousDowntimeCategory(category,isDowntime);
		
		//we build the state list for the category
		this.linkedValidatorCategories.get(indiceBPList).add(""+state);
		this.linkedValidatorCategories.get(indiceBPList).add(""+stateDown);
		this.linkedValidatorCategories.get(indiceBPList).add(""+isDowntime);
		this.linkedValidatorCategories.get(indiceBPList).add(""+gap);
		this.linkedValidatorCategories.get(indiceBPList).add(""+gapDown);

	}

	/**
	 * Compute state of MIN High Level BP for a specific category
	 * @param listStateBpByCategory
	 * @param indiceBPList 
	 * @param string 
	 * @return 
	 */
	private void determineEtHighBPAttributes(
			ArrayList<ArrayList<String>> listStateBpByCategory, String category, int indiceBPList) {
		// TODO Auto-generated method stub
		int nbDowntime = 0;
		int nbFailure = 0;
		int gap = 0;
		int gapDown = 0;
		int state = 1;
		int isDowntime = 0;
		int stateDown = 0;
		
		//warning
		//System.out.println("Nous testons une application de type ET : " + this.name);
		//listStateBpByCategory.get(i).get(0) = category
		//listStateBpByCategory.get(i).get(1) = state
		//listStateBpByCategory.get(i).get(2) = isDowntime
		//listStateBpByCategory.get(i).get(3) = gap
		for(int i=0; i < listStateBpByCategory.size(); i++){
			
			if(listStateBpByCategory.get(i).get(0).equals(category)){
			
				if(listStateBpByCategory.get(i).get(1).equals("0")){
					state = 0;
					nbFailure++;
					
					if(Integer.parseInt(listStateBpByCategory.get(i).get(2)) == 1){
						nbDowntime++;
					}
				}
			}
		}
		//We define the state of the bp for this category
		if(state != 0)
			state = 1;

		
		if(nbDowntime == nbFailure && nbDowntime > 0)
			isDowntime = 1;
		else if(nbDowntime < nbFailure && nbDowntime > 0)
			isDowntime = 2;
		else isDowntime = 0;
		
		//init
		if(this.listCategoryPreviousIsDowntime.size() == 0)
		{
			//System.out.println("J'initialise ma liste de previous downtime");
			this.replaceCurrentPreviousDowntimeCategory(category,isDowntime);
		}
			
		
		stateDown = this.computeStateDown(state, isDowntime);
		gap = this.computGapHighAppli(listStateBpByCategory, category,state,stateDown, isDowntime, gap, "normal");
		gapDown = this.computGapHighAppli(listStateBpByCategory, category, state, stateDown, isDowntime, gap, "down");
		
		this.replaceCurrentPreviousDowntimeCategory(category,isDowntime);
		//on rempli le previousIsDowntime des catégorie
		
		//we build the state list for the category
		this.linkedValidatorCategories.get(indiceBPList).add(""+state);
		this.linkedValidatorCategories.get(indiceBPList).add(""+stateDown);
		this.linkedValidatorCategories.get(indiceBPList).add(""+isDowntime);	
		this.linkedValidatorCategories.get(indiceBPList).add(""+gap);
		this.linkedValidatorCategories.get(indiceBPList).add(""+gapDown);
		
		/*We have to solution to determine downtime state
		 * If there are as many failure as downtime, failure are only due to downtime and isDowntime = 1
		 * If there are more failure than downtime, failure ae not only due to downtime and isDowntime = 2;*
		 * Else isDowntime = 0*/
	}

	/**
	 * This methods allows to replace category isDowntime or create it if not exist
	 * @param currentCategory
	 * @param currentIsDowntime
	 */
	private void replaceCurrentPreviousDowntimeCategory(String currentCategory,
			int currentIsDowntime) {
		// TODO Auto-generated method stub
		boolean categoryFind = false;
		
		for(int i = 0; i < this.listCategoryPreviousIsDowntime.size();i++)
		{
			if(this.listCategoryPreviousIsDowntime.get(i).get(0).equals(currentCategory))
			{
				categoryFind = true;
				this.listCategoryPreviousIsDowntime.get(i).remove(1);
				this.listCategoryPreviousIsDowntime.get(i).add("" + currentIsDowntime);
			}
		}
		if(!categoryFind) {
			ArrayList<String> categoryPreviousIsDowntime = new ArrayList<String>();
			categoryPreviousIsDowntime.add(currentCategory);
			categoryPreviousIsDowntime.add(currentIsDowntime + "");
			this.listCategoryPreviousIsDowntime.add(categoryPreviousIsDowntime);
		}
		
	}

	/**
	 * @param listStateBpByCategory
	 * @param category
	 * @param state
	 * @return
	 */
	private int computGapHighAppli(
			ArrayList<ArrayList<String>> listStateBpByCategory, String category, int state, int stateDown, int isDowntime, int gap, String typeCalcul) {
		
		ArrayList<Integer> listGapValidatorUp = new ArrayList <Integer>();
		ArrayList<Integer> listGapValidatorDown = new ArrayList <Integer>();
		int gapTemp = 0;
		int gapAppliTemp = 0;
		int stateTemp = 0;
		
		for(int i=0; i < listStateBpByCategory.size(); i++) {
			
			if(listStateBpByCategory.get(i).get(0).equals(category))
			{
				if(!typeCalcul.equals("down")) {
					gapTemp = Integer.parseInt(listStateBpByCategory.get(i).get(3));
					stateTemp = Integer.parseInt(listStateBpByCategory.get(i).get(1));
				}
				else {
					gapTemp = Integer.parseInt(listStateBpByCategory.get(i).get(4));
					stateTemp = this.computeStateDown(Integer.parseInt(listStateBpByCategory.get(i).get(1)),
									Integer.parseInt(listStateBpByCategory.get(i).get(2)));
				}
				
				if(stateTemp == 1){
					listGapValidatorUp.add(gapTemp);
				}
				else {	
					listGapValidatorDown.add(gapTemp);
				}			
			}
		}
		
		if(!typeCalcul.equals("down"))
		{
			if(state == 0)
				gapAppliTemp = this.computeGapDown(listGapValidatorUp,listGapValidatorDown);
			else if(state == 1)
				gapAppliTemp = this.computeGapUp(listGapValidatorUp,listGapValidatorDown);
		}
		else {
			if(stateDown == 0)
				gapAppliTemp = this.computeGapDown(listGapValidatorUp,listGapValidatorDown);
			else if(stateDown == 1 && isDowntime == 0)
				if(this.getCategoryPreviousDowntime(category).equals("1")) {
					gapAppliTemp = 0;
				}
				else gapAppliTemp = gap;
			else if(stateDown == 1 && isDowntime == 1)
				gapAppliTemp = 0;
		}
		return gapAppliTemp;
	}

	/**
	 * 
	 * @param category : category we look for previous downtime
	 * @return
	 */
	private String getCategoryPreviousDowntime(String category) {
		// TODO Auto-generated method stub
		String isDowntime = "";
		
		for(int i = 0; i < this.listCategoryPreviousIsDowntime.size(); i++)
		{
			//System.out.println("Je récupère le dernier isDowntime de la category " + category);
			if(this.listCategoryPreviousIsDowntime.get(i).get(0).equals(category)){
				//System.out.println(this.listCategoryPreviousIsDowntime.get(i).get(1));
				isDowntime = this.listCategoryPreviousIsDowntime.get(i).get(1);
			}
		}
		return isDowntime;
	}

	/**
	 * This method get state, stateDown, isDowntime based on linked application
	 * which has the category set in parameter
	 * @param category
	 * @param group_bp 
	 * @param listStateBpByCategory 
	 * @return 
	 */
	private ArrayList<ArrayList<String>> determineCategoryAttribute(group_bp group_bp, ArrayList<ArrayList<String>> listStateBpByCategory) {
		// TODO Auto-generated method stub
		
		ArrayList<String> listStateforCategory = new ArrayList<String>();
		
		for(int i = 0; i < this.validator_list.size(); i++) {
			
			if(this.validator_list.get(i).getType().equals("bp")){
					
					listStateforCategory = new ArrayList<String>();
					listStateforCategory.add(this.validator_list.get(i).getCategory(group_bp));
					listStateforCategory.add(""+this.validator_list.get(i).getState());
					listStateforCategory.add(""+this.validator_list.get(i).getIsDowntimeFlag());
					listStateforCategory.add(""+this.validator_list.get(i).getGap());
					listStateforCategory.add(""+this.validator_list.get(i).getGapDown());
					listStateBpByCategory.add(listStateforCategory);
			}	
		}
		
		return listStateBpByCategory;
		
	}

	/**
	 * This method insert values application category states into f_dtm_application_period
	 * @param fiveMinutesInterval
	 */
	public void insertCategoryFactDtmApplicationPeriod(int fiveMinutesInterval) {
		// TODO Auto-generated method stub
		
		String categoryanalysis = "";
		int state = 0;
		int stateDown = 0;
		int isDowntime = 0;
		int gap = 0;
		int gapDown = 0;
		
		for(int i=0; i < this.linkedValidatorCategories.size(); i++)
		{
			categoryanalysis = this.linkedValidatorCategories.get(i).get(0);
			state = Integer.parseInt(this.linkedValidatorCategories.get(i).get(1));
			stateDown = Integer.parseInt(this.linkedValidatorCategories.get(i).get(2));
			isDowntime = Integer.parseInt(this.linkedValidatorCategories.get(i).get(3));
			gap = Integer.parseInt(this.linkedValidatorCategories.get(i).get(4));
			gapDown = Integer.parseInt(this.linkedValidatorCategories.get(i).get(5));
			
			//warning
			//System.out.println("L'application high level " + this.name + " est state " + state + " pour la catégorie " + categoryanalysis);
			
			
			//String appli_name,
			//String source, int interval_minute, String category, int state, int isDowntime, int gap
			this.mycon.insert_fact_category_availability_application_period(this.name, this.source, fiveMinutesInterval, this.category, state, stateDown, isDowntime, gap, categoryanalysis, gapDown);
		
		}
	}

	public boolean getIsDetermined() {
		// TODO Auto-generated method stub
		return this.isDetermined;
	}

	public int getGap() {
		// TODO Auto-generated method stub
		return this.gap;
	}

	public int getGapDown() {
		// TODO Auto-generated method stub
		return this.gapDown;
	}
}












