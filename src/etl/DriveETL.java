package etl;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import dbRowObjects.DriveDriveOutcomeTableRow;
import dbRowObjects.DriveTableRow;
import postgreSQL.TableQueryFactory;

public class DriveETL {
	
	@SuppressWarnings("rawtypes")
	public static ArrayBlockingQueue<LinkedBlockingQueue> transformDriveData(LinkedBlockingQueue<String[]> rawDriveData) {
		/*
		 * the rows that can't be pulled directly from the excel data:
		 * 		GameID --> TableQueryFactory.lookupGameIDFromGameKey()
		 * 		TeamSeasonID  --> TableQueryFactory.lookupTeamIDFromName()
		 */
		
		LinkedBlockingQueue<DriveTableRow> driveData = new LinkedBlockingQueue<DriveTableRow>();
		LinkedBlockingQueue<DriveDriveOutcomeTableRow> driveDriveOutcomeData = new LinkedBlockingQueue<DriveDriveOutcomeTableRow>();
		int year = -1, gameKey = -1, driveID = 1;
		
		for(String[] row : rawDriveData) {			
			DriveTableRow drive = new DriveTableRow();
			
			try {
				year = Integer.parseInt(row[0]);
				gameKey = Integer.parseInt(row[3]);
			} catch (NumberFormatException e){
				e.printStackTrace();
				break;
			}
			
			drive.gameID = TableQueryFactory.lookupGameIDFromGameKey(year, gameKey);
			drive.possessionTeamSeasonID = TableQueryFactory.lookupTeamIDFromName(year, row[5]);
			
			try {
				drive.driveNumOfGame = Short.parseShort(row[4]);
			} catch (NumberFormatException e) {
				e.printStackTrace();	// could just set it to null
				break;
			}	
			
			drive.setDriveStartOrEndTime(true, row[6]);
			drive.setDriveStartOrEndTime(false, row[7]);
			drive.setStartField(row[8]);
			
			// having multiple outcomes (comma-separated) will change the size of our rows (if only one outcome, there will be 13 rows)
			// check the rowSize here and adjust which column# corresponds to each field
			int numOutcomes = row.length-13;
			
			drive.setEndFieldAndTotYards(row[12+numOutcomes]);
			
			try {
				drive.drivePenaltyYards = Short.parseShort(row[10+numOutcomes]);
			} catch (NumberFormatException e){
				e.printStackTrace();
				break;
			}
			
			drive.setTotPosTime(row[11+numOutcomes]);
			driveData.add(drive);
			
			String[] outcomes = new String[numOutcomes + 1];
			for(int i=0; i<numOutcomes+1; i++) {
				outcomes[i] = row[9+i];
			}
			// now populate the driveDriveOutcome associated with this drive:
			driveDriveOutcomeData = DriveDriveOutcomeTableRow.addDriveOutcomeData(driveDriveOutcomeData, driveID, outcomes);
			driveID++;
		}
		
		ArrayBlockingQueue<LinkedBlockingQueue> data = new ArrayBlockingQueue<LinkedBlockingQueue>(2); 
		data.add(driveData);
		data.add(driveDriveOutcomeData);
		
		return data;
	}
	
	
	public static void main(String[] args) {
		
	}
}