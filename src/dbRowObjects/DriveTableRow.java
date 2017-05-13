package dbRowObjects;

import java.time.LocalTime;

public class DriveTableRow {

	/*
	 * Note that this excludes homeTeamStartScore and AwayTeamSTartScore, because
	 * this data was not available from the Excel sheets pulled from NFLGame
	 */
	public Integer gameID;
	public Integer possessionTeamSeasonID;
	public Integer driveNumOfGame;
	public Integer driveStartQuarter;
	public LocalTime driveStartGameClock;
	public Integer driveStartField;
	public Integer driveEndQuarter;
	public LocalTime driveEndGameClock;
	public Integer driveEndField;
	public Integer drivePenaltyYards;
	public String driveTotPosTime;
	public Integer driveTotYards;	
	
	public void setDriveStartOrEndTime(boolean isStartTime, String rawTime) {
		if(rawTime == null || rawTime.isEmpty()) {
			this.driveStartGameClock = null;
			this.driveEndGameClock = null;
			return;
		}
		
		boolean Qflag = false;
		String time = "";
		
		for(int i=1; i<rawTime.length(); i++) {
			if(!Qflag && rawTime.charAt(i-1) == 'Q') {
				try {
					int quarter = Integer.parseInt(rawTime.substring(i, i+1));
					
					if(isStartTime)
						this.driveStartQuarter = quarter;
					else
						this.driveEndQuarter = quarter;
				} catch(NumberFormatException e) {
					e.printStackTrace();
					break;
				}
				Qflag = true;
			}
			else if(Qflag && rawTime.charAt(i) != ' ') {				
				time += rawTime.charAt(i);				
			}
		}
		
		LocalTime sqlTime = LocalTime.parse(time);
		
		if(isStartTime) {
			this.driveStartGameClock = sqlTime;
		}
		else {
			this.driveEndGameClock = sqlTime;
		}
	}
	
	// Data comes in in the range "OWN 1 - Opp 1", with "MIDFIELD" in the middle: 
	// this method converts data from that range to an int in the range of -49 to +49,
	// where a negative number corresponds to team's OWN field, and positive == OPP field
	// MIDFIELD == 0
	public void setStartField(String rawStartField) {
		if(rawStartField == null || rawStartField.length() == 0) {
			this.driveStartField = null;
			return;			
		}
		rawStartField = rawStartField.toUpperCase();
		int yardLine = -999;
		
		if(rawStartField.equals("MIDFIELD")) {
			yardLine = 0;
		} else {			
			String oppOwn = rawStartField.substring(0, 3);
			
			try {
				yardLine = Integer.parseInt(rawStartField.substring(4,rawStartField.length()));
			} catch (NumberFormatException nfe) {
				System.out.println("Error trying to parse integer - StartField - in GameID: " + this.gameID +", DriveNum: "+ this.driveNumOfGame);
				this.driveStartField = null;
				nfe.printStackTrace();
				return;
			}
			
			if(yardLine <= -50 || yardLine >= 50) {
				throw new IllegalArgumentException("Error parsing integer - startField outside of valid values in GameID: " + this.gameID +", DriveNum: "+ this.driveNumOfGame);		
			}
			
			if(oppOwn.equals("OWN")) {
				yardLine -= 50;
			}
		}
		
		this.driveStartField = yardLine;
	}
	
	public void setEndFieldAndTotYards(String rawDriveTotYards) {
		int driveTotYards;
				
		if(this.driveStartField == null) {
			this.driveEndField = null;
			this.driveTotYards = null;
			return;
		}
		
		try {
			driveTotYards = Integer.parseInt(rawDriveTotYards);
		} catch (NumberFormatException e){
			e.printStackTrace();
			this.driveEndField = null;
			this.driveTotYards = null;
			return;
		}
		
		if(driveTotYards < -99 || driveTotYards > 99)
			throw new IllegalArgumentException("Error: arg 'driveTotYards' is outside of the valid range");
		
		int endField = this.driveStartField + driveTotYards;
		if(endField < -50 || endField > 50) {
			this.driveEndField = null;
			this.driveTotYards = null;
		}
		else {
			this.driveEndField = endField;
			this.driveTotYards = driveTotYards;
		}
	}
	
	public void setTotPosTime(String rawTotPosTime) {
		if(rawTotPosTime == null || rawTotPosTime.length() == 0) {
			this.driveTotPosTime = null;
			return;
		}
		rawTotPosTime = rawTotPosTime.trim();
		
		// test the first char and the last 2 chars of the string to make sure they are ints.
		// if not, set to null & return
		try {
			@SuppressWarnings("unused")
			int testDigit = Integer.parseInt(rawTotPosTime.substring(0, 1));
			testDigit = Integer.parseInt(rawTotPosTime.substring(rawTotPosTime.length()-2));			
		} catch (NumberFormatException e) {
			this.driveTotPosTime = null;
			return;
		}
		
		// if time String doesn't contain a colon, set value to null and return
		if(rawTotPosTime.charAt(1) != ':' && rawTotPosTime.charAt(2) != ':') {
			this.driveTotPosTime = null;
			return;
		}
		
		// in raw data, when startField is unknown, python parses this string as "0:00"
		// if this is the case, we need to check to see if it should actually be set to null
		if(rawTotPosTime.equals("0:00")) {
			if(this.driveStartField == null) {
				this.driveTotPosTime = null;
				return;
			}			
		}
		
		this.driveTotPosTime = rawTotPosTime;
	}
	
}