import java.util.ArrayList;


public class group_bp {

	private ArrayList<bp> group_bp_list = new ArrayList<bp>();
	
	public group_bp()
	{
		this.group_bp_list = new ArrayList<bp>();
	}
	
	/***
	 * Add a new bp to group_bp_list
	 */
	public void  add_group(bp my_bp){
		
		this.group_bp_list.add(my_bp);
		
	}

	/**
	 * This method retrieve into database unavailability logs for each host service
	 * for each bp validator for each five minutes interval of given date
	 * @param my_EON_log 
	 * @param mycon 
	 */
	public void compute_availability(EON_log my_EON_log, EON_log_downtime my_EON_log_downtime, mysql_connection mycon) {
		
		int increment = 1;
		int i = share_variable.born_from;
		int five_minutes_interval;
		int nb_application = 0;
		int nbtours = 1;
		int incrementTour = 0;
		while(i < share_variable.born_to)
		{
			// five minute interval correspon to i * 5
			five_minutes_interval = i*5;
			
			//if a loop has been already done with this five minutes interval, inccrement = 0, 
			//five_minutes_logs has been alreadycreated
			if(increment != 0){
				
				
				//log importation for a five minute interval
				my_EON_log = new EON_log();
				my_EON_log = mycon.import_logs(share_variable.export_date, five_minutes_interval, my_EON_log);
				
				//log importation for a five minute interval
				my_EON_log_downtime = new EON_log_downtime();
				my_EON_log_downtime = mycon.import_logs_downtimme(share_variable.export_date, five_minutes_interval, my_EON_log_downtime);
				
				//on ne réinit les state que si on a incrémenté
				if(i> share_variable.born_from) {
					nbtours = 1;
					nb_application = 0;
					incrementTour = 0;
					this.reinit_states();
				}
				
				
				
			}
			increment = 1;
			
			nbtours = nbtours + incrementTour;
					
			
			for(int j=0; j < this.group_bp_list.size(); j++)
			{
				
				if(!this.group_bp_list.get(j).getValidator_state_test())
				{
					/**if(this.group_bp_list.get(j).getName().equals("vmpub20_BasePub") ||
							this.group_bp_list.get(j).getName().equals("vmpub21_BasePub") ||
									this.group_bp_list.get(j).getName().equals("vmpub22_BasePub"))
					{
						System.out.println("Je calcule l'état de la BP " + 
								this.group_bp_list.get(j).getName() +
									" pour le code interval " + i*5);
					}*/
					//System.out.println("Le bp " + this.group_bp_list.get(j).getName() + " nous testons ses validateurs");
					this.group_bp_list.get(j).compute(my_EON_log, my_EON_log_downtime, five_minutes_interval, this);
				}
				//warning
				//else System.out.println("L'application " + this.group_bp_list.get(j).getName() + " a le statut " + this.group_bp_list.get(j).getState());
				
				if(!this.group_bp_list.get(j).getValidator_state_test())
				{
					if(increment != 0) incrementTour++;
					//warning
					//System.out.println("La bp de type "+ this.group_bp_list.get(j) + " n'est pas valide. Tour " + nbtours);
					increment = 0;
				}
				else {
					
					//We insert into database for this bp only if this bp has not been inserted for this interval
					if(!this.group_bp_list.get(j).getIsDetermined()){
						nb_application ++;
						this.group_bp_list.get(j).determineBpAttribute(five_minutes_interval, i, nb_application, this.group_bp_list.size(), nbtours);
					}
				}			
			}
			if(increment != 0){
				this.determineAttributesHighLevelAppli(this, five_minutes_interval);
			}
			i += increment;
			
		}
	}
	
	/**
	 * Determine for each HighLevelBP the state
	 * @param group_bp 
	 * @param fiveMinutesInterval 
	 */
	private void determineAttributesHighLevelAppli(group_bp group_bp, int fiveMinutesInterval) {
		// TODO Auto-generated method stub
		
		ArrayList<bp> highBpGroup = share_variable.highLvlGroupBp.getGroup();
		
		for(int i = 0; i < highBpGroup.size(); i++){
			highBpGroup.get(i).determineListCategoryAttributes(this);	
			highBpGroup.get(i).insertCategoryFactDtmApplicationPeriod(fiveMinutesInterval);
		}
		
	}

	/**
	 * This method re initialize all appli and validators states
	 */
	private void reinit_states() {
		// TODO Auto-generated method stub
		for(int i=0; i < this.group_bp_list.size(); i++)
		{
			this.group_bp_list.get(i).reinit();
		}
	}

	/**
	 * 
	 * @param appli_name
	 * @return appli state
	 * This mmethod allows to return appli_name current state
	 * @throws missing_application 
	 */
	public int test_application_state(String appli_name, String appli_source) 
	{
		int state = -1;
		
		for (int i=0; i < this.group_bp_list.size(); i++)
		{
			String name = this.group_bp_list.get(i).getName();
			String source = this.group_bp_list.get(i).getSource();
			if(appli_name.equals(name) && appli_source.equals(source))
			{
				state = this.group_bp_list.get(i).getState();
			}
		}
		
		/*
		if(state == -1)
		{
			throw new missing_application("L'application " + appli_name +" source " + appli_source + " présente dans la table bp_links n'est" +
					" pas présente dans la table bp");	
		}
		*/
		return state;
	}
	
	/**
	 * This method test downtime state for application filled in parameter
	 * @param appli_name
	 * @param appli_source
	 * @return
	 */
	public int test_application_downtime(String appli_name,
			String appli_source) {
		// TODO Auto-generated method stub
		int isDowntime = -1;
		
		for (int i=0; i < this.group_bp_list.size(); i++)
		{
			String name = this.group_bp_list.get(i).getName();
			String source = this.group_bp_list.get(i).getSource();
			
			if(appli_name.equals(name) && appli_source.equals(source))
			{
				isDowntime = this.group_bp_list.get(i).getDowntime();
			}
		}
		
		/*
		if(isDowntime == -1)
		{
			throw new missing_application("L'application " + appli_name +" source " + appli_source + " présente dans la table bp_links n'est" +
					" pas présente dans la table bp");	
		}
		*/
		return isDowntime;
	}
	
	
	/**
	 * For each application we create linked validators
	 */
	public void createValidators() {
		// TODO Auto-generated method stub
		for(int i=0 ; i< this.group_bp_list.size(); i++)
		{
			this.group_bp_list.get(i).createValidators();
		}
	}

	public ArrayList<bp> getGroup() {
		// TODO Auto-generated method stub
		return this.group_bp_list;
	}

	/**
	 * 
	 * @param bp_name
	 * @param bp_source
	 * @return
	 */
	public String getBpCategory(String bp_name, String bp_source) {
		// TODO Auto-generated method stub
		String category = "null";
		
		for(int i = 0; i < this.group_bp_list.size(); i++){
			
			if(this.group_bp_list.get(i).getName().equals(bp_name) && this.group_bp_list.get(i).getSource().equals(bp_source)){
				category = this.group_bp_list.get(i).getCategory();	
			}
		}
		
		return category;
	}

	public int getApplicationGap(String appli_name, String appli_source) {
		// TODO Auto-generated method stub
		int gap = 0;
		
		for (int i=0; i < this.group_bp_list.size(); i++)
		{
			String name = this.group_bp_list.get(i).getName();
			String source = this.group_bp_list.get(i).getSource();
			if(appli_name.equals(name) && appli_source.equals(source))
			{
				gap = this.group_bp_list.get(i).getGap();
			}
		}	
		
		return gap;
		
	}

	public int getApplicationGapDown(String appli_name, String appli_source) {
		// TODO Auto-generated method stub
		int gapDown = 0;
		
		for (int i=0; i < this.group_bp_list.size(); i++)
		{
			String name = this.group_bp_list.get(i).getName();
			String source = this.group_bp_list.get(i).getSource();
			if(appli_name.equals(name) && appli_source.equals(source))
			{
				gapDown = this.group_bp_list.get(i).getGapDown();
			}
		}	
		
		return gapDown;		
	}


}
