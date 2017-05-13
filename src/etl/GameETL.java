package etl;

import java.util.concurrent.LinkedBlockingQueue;

import dbRowObjects.GameTableRow;
import postgreSQL.TableQueryFactory;

public class GameETL {

	/*
	 * This method takes the raw data (already read in from file) and transforms it to match the format
	 * expected by the SQL table. That format is as follows:
	 * GameKey,SeasonYear,Week,Month,Day,WeekDay,GameStartTime,ForeignCityID,HomeTeamID,HomeTeamFirstDowns,HomeTeamTotYds,HomeTeamPassYds,HomeTeamRushYds,HomeTeamTurnovers,
	 * HomeTeamPenaltyCnt,HomeTeamPenaltyYds,HomeTeamPuntCnt,HomeTeamPuntYds,HomeTeamPuntAvg,HomeTeamFinalScore,AwayTeamID,AwayTeamFirstDowns,AwayTeamTotYds,
	 * AwayTeamPassYds,AwayTeamRushYds,AwayTeamTurnovers,AwayTeamPenaltyCnt,AwayTeamPenaltyYds,AwayTeamPuntCnt,AwayTeamPuntYds,AwayTeamPuntAvg,AwayTeamFinalScore
	 */
	public static LinkedBlockingQueue<GameTableRow> transformGameData(LinkedBlockingQueue<String[]> raw) {
		int rowsProcessed = 1;
		LinkedBlockingQueue<GameTableRow> transformedData = new LinkedBlockingQueue<GameTableRow>();
				
		int year;
		
		for(String[] row : raw) {			
			System.out.println("Begin processing row #"+ rowsProcessed);
			GameTableRow transformedRow = new GameTableRow();
			
			year = Integer.parseInt(row[1]);
			transformedRow.GameKey = Integer.parseInt(row[0]);															//GameKey
			transformedRow.SeasonYear = year;																			//Year			
			transformedRow.Week = Integer.parseInt(row[2]);																//Week
			transformedRow.ForeignCityID = null;
			
			// get rows[3] through[6] from GameScheduleInfo	- {Month, Day, WeekDay, GameStartTime }						//REST_OF_SCHEDULE info
			if(year <= 2014) {
				transformedRow.populateScheduleData(row[34],row[39],row[38],row[36],null);														
			}
			else {
				transformedRow.populateScheduleData(row[35],row[40],row[39],row[37],row[30]);
			}
			transformedRow.HomeTeamFinalScore = Integer.parseInt(row[15]);												//HomeTeamFinalScore
			transformedRow.AwayTeamFinalScore = Integer.parseInt(row[28]);												//AwayTeamFinalScore
			
			transformedRow.HomeTeamID = TableQueryFactory.lookupTeamIDFromName(year, row[3]);													//HomeTeamID
			transformedRow.AwayTeamID = TableQueryFactory.lookupTeamIDFromName(year,row[16]);													//AwayTeamID
			
			transformedRow.populateHomeTeamData(row[4],row[5],row[6],row[7],row[10],row[8],row[9],row[11],row[12],row[13]);															//REST_OF_HOME_TEAM info
			transformedRow.populateAwayTeamData(row[17],row[18],row[19],row[20],row[23],row[21],row[22],row[24],row[25],row[26]);
			
			transformedData.add(transformedRow);
			
			rowsProcessed++;			
		}		
		
		System.out.println("Successfully processed "+ rowsProcessed +" rows of data.");
		
		return transformedData;
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
