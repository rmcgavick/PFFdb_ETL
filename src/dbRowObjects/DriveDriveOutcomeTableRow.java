package dbRowObjects;

import java.util.concurrent.LinkedBlockingQueue;

import postgreSQL.TableQueryFactory;


public class DriveDriveOutcomeTableRow {

	public Integer driveID;
	public Integer driveOutcomeID;
	public Integer driveOutcomeOrder;
	
	// DriveOutcomeID --> TableQueryFactory.lookupDriveOutcomeIDFromName()
	public DriveDriveOutcomeTableRow(int driveID, String rawDriveOutcome, int outcomeNum) {
		this.driveID = driveID;			
		this.driveOutcomeID = TableQueryFactory.lookupDriveOutcomeIDFromName(rawDriveOutcome);		
		this.driveOutcomeOrder = outcomeNum;							
	}
	
	public static LinkedBlockingQueue<DriveDriveOutcomeTableRow> addDriveOutcomeData(LinkedBlockingQueue<DriveDriveOutcomeTableRow> ddoQ, int driveID, String[] outcomes) {
		if(outcomes == null || outcomes.length == 0) {
			DriveDriveOutcomeTableRow ddo = new DriveDriveOutcomeTableRow(driveID, "UNKNOWN", 1);			
			ddoQ.add(ddo);
			return ddoQ;
		}
		int driveOutcomeCount = 1;
		
		for(String outcome : outcomes) {
			outcome = outcome.trim().toUpperCase();
			char first = outcome.charAt(0), last = outcome.charAt(outcome.length()-1);
			if(first == '"')
				outcome = outcome.substring(1);
			if(last == '"')
				outcome = outcome.substring(0, outcome.length()-1);
			if(outcome.equals("DOWNS"))
				outcome = "TURNOVER ON DOWNS";
			else if(outcome.equals("MISSED FG"))
				outcome = "MISSED FIELD GOAL";
			else if(outcome.equals("BLOCKED FG"))
				outcome = "BLOCKED FIELD GOAL";
			DriveDriveOutcomeTableRow ddo = new DriveDriveOutcomeTableRow(driveID, outcome, driveOutcomeCount);
			ddoQ.add(ddo);
			driveOutcomeCount++;
		}
		
		return ddoQ;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
