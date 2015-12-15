import java.util.ArrayList;


public class EON_log {

	private ArrayList <ArrayList<String>> eon_log_table;
	
	public EON_log(){
		
		eon_log_table = new ArrayList<ArrayList<String>>();
		
	}

	/***
	 * 
	 * @param log_row
	 */
	public void add(ArrayList<String> log_row) {
		// TODO Auto-generated method stub
		this.eon_log_table.add(log_row);
		
	}

	public ArrayList <ArrayList<String>> getEon_log_table() {
		// TODO Auto-generated method stub
		return this.eon_log_table;
	}
	
}
