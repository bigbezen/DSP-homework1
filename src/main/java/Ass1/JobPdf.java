package Ass1;

public class JobPdf {
	public enum Action {ToImage, ToHTML, ToText};
	
	private String link;
	private Action action;
	
	private JobPdf(String link, Action action){
		this.action = action;
		this.link = link;
	}
	
	public String getLink(){
		return this.link;
	}
	
	public Action getAction(){
		return this.action;
	}
	
	public static JobPdf createNewJob(String textJob){
		String[] parse = new String[2];
		parse = textJob.split("\t");
		return new JobPdf(parse[1], Action.valueOf(parse[0]));
	}
}
