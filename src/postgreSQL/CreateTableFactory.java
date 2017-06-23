package postgreSQL;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import dbRowObjects.DriveDriveOutcomeTableRow;
import dbRowObjects.DriveTableRow;
import dbRowObjects.GameTableRow;
import etl.DriveETL;
import etl.GameETL;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class CreateTableFactory {
	static Connection c;
		
	private static void createConnection() {
		System.out.println("Attempting to connect to database...");
		
		Properties dbProps = new Properties();
		try {
			FileInputStream in = new FileInputStream("src/postgreSQL/db.properties");
			dbProps.load(in);
			in.close();
		} catch (IOException e1) {
			e1.printStackTrace();
			System.exit(-1);
		}
		
		String connStr = dbProps.getProperty("connectionString");
		String user = dbProps.getProperty("username");
		String pass = dbProps.getProperty("password");
		
		try {
			Class.forName("org.postgresql.Driver");
			c = DriverManager.getConnection(connStr,user,pass);
		}
		catch (Exception e) {
			e.printStackTrace();
			System.err.println(e.getClass().getName()+ ": "+e.getMessage());
			System.exit(0);
		}
		System.out.println("Success! Database connection established.");
	}
	
	private static LinkedBlockingQueue<String[]> readFromCSVFile(String path, boolean hasHeaders) throws IOException {		
		BufferedReader br = new BufferedReader(new FileReader(path));
		
		LinkedBlockingQueue<String[]> tempQ = new LinkedBlockingQueue<String[]>();
		
		String currLine;
		String[] inputData;		
		
		if(hasHeaders)
			currLine = br.readLine();
		
		while((currLine = br.readLine()) != null) {			
			// split currLine using regex
			inputData = currLine.trim().split(",");
			tempQ.add(inputData);
		}
		
		br.close();

		return tempQ;
	}
	
	public static void insertDriveOutcomeDataFromFile(String filepath) {
		createConnection();
		LinkedBlockingQueue<String[]> rawDriveData = new LinkedBlockingQueue<String[]>();
		
		try {
			rawDriveData = readFromCSVFile(filepath, true);
		}
		catch (IOException ioe){
			ioe.printStackTrace();
			System.exit(-1);
		}
		
		@SuppressWarnings("rawtypes")
		ArrayBlockingQueue<LinkedBlockingQueue> driveAndOutcomeData = DriveETL.transformDriveData(rawDriveData);

		//populate the Drive SQL table
		@SuppressWarnings("unchecked")
		LinkedBlockingQueue<DriveTableRow> driveData = driveAndOutcomeData.poll();		
		PreparedStatement stmt;
		int i = 0;
		
		try {			 
			// TODO: change this to insert into the real Drive table, after testing is complete
			 String sql = "INSERT INTO \"TEST_Drive\" (\"GameID\",\"PossessionTeamID\",\"DriveSequenceNum\",\"PosTeamDriveStartScore\","
			 			+ "\"OppTeamDriveStartScore\",\"DriveStartQuarter\",\"DriveStartField\",\"DriveEndQuarter\","
			 			+ "\"DriveEndField\",\"DriveTotYds\",\"DriveTotPenaltyYds\",\"DriveStartTimeMinute\",\"DriveStartTimeSecond\","
			 			+ "\"DriveEndTimeMinute\",\"DriveEndTimeSecond\",\"DriveTotPosTimeMinute\",\"DriveTotPosTimeSecond\")"
			 			+ "VALUES (? ? ? ? ? ? ? ? ? ? ? ? ? ? ? ? ?)";
			 
			 System.out.println("Inserting "+ driveData.size() +" rows...");
			 for(DriveTableRow row : driveData) {				 
				 row = driveData.poll();
				 ////////////////////////////////////////////////////////////////////
				 stmt = c.prepareStatement(sql);
				 stmt.setInt(1, row.gameID);
				 stmt.setInt(2, row.possessionTeamSeasonID);
				 stmt.setInt(3, row.driveNumOfGame);
				 stmt.setNull(4, java.sql.Types.SMALLINT);
				 stmt.setNull(5, java.sql.Types.SMALLINT);
				 if(row.driveStartQuarter != null)
					 stmt.setInt(6, row.driveStartQuarter);
				 else
					 stmt.setNull(6, java.sql.Types.SMALLINT);
				 if(row.driveStartField != null)
					 stmt.setInt(7, row.driveStartField);
				 else
					 stmt.setNull(7, java.sql.Types.SMALLINT);
				 if(row.driveEndQuarter != null)
					 stmt.setInt(8, row.driveEndQuarter);
				 else
					 stmt.setNull(8, java.sql.Types.SMALLINT);
				 if(row.driveEndField != null)
					 stmt.setInt(9, row.driveEndField);
				 else
					 stmt.setNull(9, java.sql.Types.SMALLINT);
				 if(row.driveTotYards != null)
					 stmt.setInt(10, row.driveTotYards);
				 else
					 stmt.setNull(10, java.sql.Types.SMALLINT);
				 if(row.drivePenaltyYards != null)
					 stmt.setInt(11, row.drivePenaltyYards);
				 else
					 stmt.setNull(11, java.sql.Types.SMALLINT);
				 if(row.driveStartGameClockMin != null)
					 stmt.setInt(12, row.driveStartGameClockMin);
				 else
					 stmt.setNull(12, java.sql.Types.SMALLINT);
				 if(row.driveStartGameClockSec != null)
					 stmt.setInt(13, row.driveStartGameClockSec);
				 else
					 stmt.setNull(13, java.sql.Types.SMALLINT);
				 if(row.driveEndGameClockMin != null)
					 stmt.setInt(14, row.driveEndGameClockMin);
				 else
					 stmt.setNull(14, java.sql.Types.SMALLINT);
				 if(row.driveEndGameClockSec != null)
					 stmt.setInt(15, row.driveEndGameClockSec);
				 else
					 stmt.setNull(15, java.sql.Types.SMALLINT);
				 if(row.driveTotPosTimeMin != null)
					 stmt.setInt(16, row.driveTotPosTimeMin);
				 else
					 stmt.setNull(16, java.sql.Types.SMALLINT);
				 if(row.driveTotPosTimeSec != null)
					 stmt.setInt(17, row.driveTotPosTimeSec);
				 else
					 stmt.setNull(17, java.sql.Types.SMALLINT);
				 //////////////////////////////////////////////////////////////////////
				 stmt.executeUpdate();
				 
				 i++;			 
			 }
			 System.out.println("Successfully inserted all Drive data");
		} catch (SQLException e) {
			e.printStackTrace();
			System.err.println("SQLState: " + e.getSQLState());
			System.err.println("Error code: " + e.getErrorCode());
			System.err.println("Error on row " + i + "; " + e.getClass().getName()+ ": "+ e.getMessage());
			Throwable t = e.getCause();
			while(t != null) {
				System.out.println("Cause: " + t);
				t = t.getCause();
			}
			System.exit(-1);
		}
		
		// now populate the DriveDriveOutcome table
		@SuppressWarnings("unchecked")
		LinkedBlockingQueue<DriveDriveOutcomeTableRow> driveOutcomeData = driveAndOutcomeData.poll();
		stmt = null;
		i = 0;
		
		try {			 
			// TODO: change this to insert into the real Drive_Drive_Outcome table, after testing is complete
			 String sql = "INSERT INTO \"TEST_Drive_Drive_Outcome\" (\"DriveDriveOutcomeID\",\"DriveID\","
			 			+ "\"DriveOutcomeID\",\"DriveOutcomeOrder\")"
			 			+ "VALUES (? ? ? ?)";
			 
			 System.out.println("Inserting "+ driveOutcomeData.size() +" rows...");
			 for(DriveDriveOutcomeTableRow row : driveOutcomeData) {				 
				 row = driveOutcomeData.poll();

				 stmt = c.prepareStatement(sql);
				 stmt.setInt(1, row.driveID);
				 stmt.setInt(2, row.driveOutcomeID);
				 stmt.setInt(3, row.driveOutcomeOrder);				 
				 stmt.executeUpdate();
				 
				 i++;			 
			 }			 
		} catch (SQLException e) {
			e.printStackTrace();
			System.err.println("SQLState: " + e.getSQLState());
			System.err.println("Error code: " + e.getErrorCode());
			System.err.println("Error on row " + i + "; " + e.getClass().getName()+ ": "+ e.getMessage());
			Throwable t = e.getCause();
			while(t != null) {
				System.out.println("Cause: " + t);
				t = t.getCause();
			}
			System.exit(-1);
		}
	
	System.out.println("Closing connection.");
	}
	
	public static void insertGameDataFromFile(String filepath) {
		createConnection();
		LinkedBlockingQueue<String[]> rawGameData = new LinkedBlockingQueue<String[]>();
		
		try {
			rawGameData = readFromCSVFile(filepath, false);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		LinkedBlockingQueue<GameTableRow> gameData = GameETL.transformGameData(rawGameData);
		
		Statement stmt = null;
		@SuppressWarnings("unused")
		int i = 0;
		
		try {
			System.out.println("Preparing SQL statement...");
			 stmt = c.createStatement();
			 String sql;			 
			 
			 System.out.println("Inserting "+ gameData.size() +" rows...");
			 for(GameTableRow row : gameData) {				 
				 row = gameData.poll();
				 sql = "INSERT INTO \"Game\" (\"GameKey\",\"SeasonYear\",\"Week\",\"Month\",\"Day\",\"WeekDay\",\"GameStartTime\",\"ForeignCityID\",\"HomeTeamID\",\"HomeTeamFirstDowns\",\"HomeTeamTotYds\",\"HomeTeamPassYds\",\"HomeTeamRushYds\",\"HomeTeamTurnovers\",\"HomeTeamPenaltyCnt\",\"HomeTeamPenaltyYds\",\"HomeTeamPuntCnt\",\"HomeTeamPuntYds\",\"HomeTeamPuntAvg\",\"HomeTeamFinalScore\",\"AwayTeamID\",\"AwayTeamFirstDowns\",\"AwayTeamTotYds\",\"AwayTeamPassYds\",\"AwayTeamRushYds\",\"AwayTeamTurnovers\",\"AwayTeamPenaltyCnt\",\"AwayTeamPenaltyYds\",\"AwayTeamPuntCnt\",\"AwayTeamPuntYds\",\"AwayTeamPuntAvg\",\"AwayTeamFinalScore\")"
						 + "VALUES (\'"+ row.GameKey +"\',\'"+ row.SeasonYear +"\',\'"+ row.Week +"\',\'"+ row.Month +"\',\'"+ row.Day +"\',\'"+ row.WeekDay+"\',\'"+ row.GameStartTime +"\',null,\'"+ row.HomeTeamID +"\',\'"+ row.HomeTeamFirstDowns +"\',\'"+ row.HomeTeamTotYds +"\',\'"+ row.HomeTeamPassYds +"\',\'"+ row.HomeTeamRushYds +"\',\'"+ row.HomeTeamTurnovers +"\',\'"+ row.HomeTeamPenaltyCnt +"\',\'"+ row.HomeTeamPenaltyYds+"\',\'"+ row.HomeTeamPuntCnt +"\',\'"+ row.HomeTeamPuntYds +"\',\'"+ row.HomeTeamPuntAvg +"\',\'"+ row.HomeTeamFinalScore +"\',\'"+ row.AwayTeamID +"\',\'"+ row.AwayTeamFirstDowns +"\',\'"+ row.AwayTeamTotYds +"\',\'"+ row.AwayTeamPassYds +"\',\'"+ row.AwayTeamRushYds +"\',\'"+ row.AwayTeamTurnovers +"\',\'"+ row.AwayTeamPenaltyCnt +"\',\'"+ row.AwayTeamPenaltyYds +"\',\'"+ row.AwayTeamPuntCnt +"\',\'"+ row.AwayTeamPuntYds +"\',\'"+ row.AwayTeamPuntAvg +"\',\'"+ row.AwayTeamFinalScore +"\')";
				 stmt.executeUpdate(sql);
				 i++;
			 }
		} catch (SQLException e) {
			e.printStackTrace();
			System.err.println(e.getClass().getName()+ ": "+e.getMessage());
			System.exit(0);
		}
		
		System.out.println("Closing connection.");
	}
	
	/* DEPRECATED: this method needs to be refactored to use PreparedStatements - see insertDriveOutcomeDataFromFile() for example */
	public static void insertPlayerPositionDataToTempTable(ConcurrentHashMap<String,String> playerData) {
		createConnection();
		Statement stmt = null;
		
		try {
			System.out.println("Preparing SQL statement...");
			 stmt = c.createStatement();
			 String sql;			 
			 
			 System.out.println("Inserting "+ playerData.size() +" rows...");
			 
			 @SuppressWarnings("rawtypes")
			 Iterator it = playerData.entrySet().iterator();
			 while(it.hasNext()) {
				 @SuppressWarnings("unchecked")
				 Map.Entry<String,String> pair = (Map.Entry<String,String>)it.next();
				 String[] row = new String[3];
				 String fullName = pair.getKey();
				 
				 Pattern pattern = Pattern.compile(", *");
				 Matcher matcher = pattern.matcher(fullName);
				 if(matcher.find()) {
					 row[1] = fullName.substring(0, matcher.start());
					 row[0] = fullName.substring(matcher.end());
				 }
				 
				 row[2] = pair.getValue();
				 
				 // sanitize all the strings in row[]
				 
				 for(int i=0; i<row.length; i++) {
					 row[i] = row[i].replace("'", "''");					 
				 }
				 
				 sql = "INSERT INTO \"Player_Positions_Temp\" (\"FirstName\",\"LastName\",\"Position\")"
						 + "VALUES (\'"+ row[0] +"\',\'"+ row[1] +"\',\'"+ row[2] +"\')";
				 stmt.executeUpdate(sql);
				 
				 it.remove();
			 }
		} catch (SQLException e) {
			e.printStackTrace();
			System.err.println(e.getClass().getName()+ ": "+e.getMessage());
			System.exit(0);
		}
		
		System.out.println("Closing connection.");
	}
	
	/* DEPRECATED: this method needs to be refactored to use PreparedStatements - see insertDriveOutcomeDataFromFile() for example */
	public static void insertPlayerDataToTempTable(LinkedBlockingQueue<String[]> playerData) {
		createConnection();
		Statement stmt = null;
		@SuppressWarnings("unused")
		int i = 0;
		
		try {
			System.out.println("Preparing SQL statement...");
			 stmt = c.createStatement();
			 String sql;			 
			 
			 System.out.println("Inserting "+ playerData.size() +" rows...");
			 for(String[] row : playerData) {				 
				 row = playerData.poll();
				 sql = "INSERT INTO \"Player_Temp_Raw\" (\"FirstName\",\"LastName\",\"Team\",\"Position\",\"JerseyNumber\",\"Birthdate\",\"College\",\"Height\",\"Weight\",\"YearsPro\",\"Status\",\"ProfileID\",\"ProfileURL\",\"GSISid\",\"GSISname\")"
						 + "VALUES (\'"+ row[2] +"\',\'"+ row[3] +"\',\'"+ row[4] +"\',\'"+ row[5] +"\',\'"+ row[6] +"\',\'"+ row[7] +"\',\'"+ row[8] +"\',\'"+ row[9] +"\',\'"+ row[10] +"\',\'"+ row[11] +"\',\'"+ row[12] +"\',\'"+ row[13] +"\',\'"+ row[14] +"\',\'"+ row[0] +"\',\'"+ row[1] +"\')";
				 stmt.executeUpdate(sql);
				 i++;
			 }
		} catch (SQLException e) {
			e.printStackTrace();
			System.err.println(e.getClass().getName()+ ": "+e.getMessage());
			System.exit(0);
		}
		
		System.out.println("Closing connection.");
	}
	
	/* DEPRECATED: this method needs to be refactored to use PreparedStatements - see insertDriveOutcomeDataFromFile() for example */
	public static void insertTeamSeasonData(LinkedBlockingQueue<int[]> teamSeasonData) {
		createConnection();
		Statement stmt = null;
		
		try {
			 System.out.println("Preparing SQL statement...");
			 stmt = c.createStatement();
			 String sql;
			 int i = 0;
			 
			 System.out.println("Inserting "+ teamSeasonData.size() +" rows...");
			 for(int[] row : teamSeasonData) {
				 row = teamSeasonData.poll();
				 sql = "INSERT INTO \"Team_Season\" (\"TeamCityID\",\"SeasonYear\",\"DivisionConferenceID\",\"HeadCoachID\",\"OffensiveCoordinatorID\",\"DefensiveCoordinatorID\")"
						 + "VALUES ("+ row[0] +","+ row[1] +","+ row[2] +","+ row[3] +","+ row[4] +","+ row[5] +")";
				 stmt.executeUpdate(sql);
				 i++;
			 }
			 
			 System.out.println("Successfully inserted "+ i +" rows.");
			 stmt.close();
			 //c.commit(); -- Auto-commit is enabled
			 c.close();
			 
		} catch (SQLException e) {
			e.printStackTrace();
			System.err.println(e.getClass().getName()+ ": "+e.getMessage());
			System.exit(0);
		}
		
		System.out.println("Closing connection.");
	}
	
	public static void main(String[] args) {
		//insertTeamSeasonData(TeamTemporalETL.getTableData());
		//insertPlayerDataToTempTable(PlayerETL.getTempTableData());
		//insertPlayerPositionDataToTempTable(GetPlayerPositionDataFromWeb.getAllPlayersByNameSinceYear(2009));
		//insertGameDataFromFile("/C:/users/Riley/workspace/PFFDB_Data_Load/dataimport/AllGameData.csv");
		
		insertDriveOutcomeDataFromFile("/C:/users/Riley/workspace/PFFDB_Data_Load/dataimport/TestDriveData.csv");
	}
}
