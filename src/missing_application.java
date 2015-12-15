
public class missing_application extends Exception {
	
	private String message;
	
	public missing_application(String message){
		
		this.message=message;
		
	}
	
	public void print()
	{
		System.out.println(this.message);
	}

}
