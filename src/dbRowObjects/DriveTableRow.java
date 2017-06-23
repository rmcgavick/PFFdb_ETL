package dbRowObjects;

public class DriveTableRow {

	/*
	 * Note that this excludes homeTeamStartScore and AwayTeamSTartScore, because
	 * this data was not available from the Excel sheets pulled from NFLGame
	 */
	public Integer gameID;
	public Integer possessionTeamSeasonID;
	public Integer driveNumOfGame;
	public Integer driveStartQuarter;
	public Integer driveStartGameClockMin;
	public Integer driveStartGameClockSec;
	public Integer driveStartField;
	public Integer driveEndQuarter;
	public Integer driveEndGameClockMin;
	public Integer driveEndGameClockSec;
	public Integer driveEndField;
	public Integer drivePenaltyYards;
	public Integer driveTotYards;
	public Integer driveTotPosTimeMin;
	public Integer driveTotPosTimeSec;
	
	public void setDriveStartOrEndTime(boolean isStartTime, String rawTime) {
		if(rawTime == null || rawTime.isEmpty()) {
			this.driveStartGameClockMin = null;
			this.driveStartGameClockSec = null;
			this.driveEndGameClockMin = null;
			this.driveEndGameClockSec = null;
			return;
		}
		
		boolean Qflag = false;
		String time = "";
		int minStr = -1, secStr = -1;
		
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
		// try convert min and sec from time string
		try {
			minStr = Integer.parseInt(time.substring(0,2));
			secStr = Integer.parseInt(time.substring(3));
		} catch (NumberFormatException nfe) {
			nfe.printStackTrace();
			System.out.println("Error converting time String: [" + time + "] to a number");
			System.exit(-1);
		}		
		
		if(isStartTime) {
			this.driveStartGameClockMin = minStr;
			this.driveStartGameClockSec = secStr;
		}
		else {
			this.driveEndGameClockMin = minStr;
			this.driveEndGameClockSec = secStr;
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
		// First try to calculate the totPosTime by taking startTime - EndTime: this should be
		// faster then converting from strings
		if(this.driveStartGameClockMin != null && this.driveStartGameClockSec != null
				&& this.driveEndGameClockMin != null && this.driveEndGameClockSec != null) {
			int min = this.driveStartGameClockMin - this.driveEndGameClockMin;
			int sec = this.driveStartGameClockSec - this.driveEndGameClockSec;
			if(sec < 0) {
				min = (short) (min-1);
				sec = (short) (60 + sec);
			}
			this.driveTotPosTimeMin = min;
			this.driveTotPosTimeSec = sec;
			return;
		}
		// if we are missing one of our values for start / end time, try to parse the string
		if(rawTotPosTime == null || rawTotPosTime.length() == 0) {
			this.driveTotPosTimeMin = null;
			this.driveTotPosTimeSec = null;
			return;
		}
		rawTotPosTime = rawTotPosTime.trim();
		
		// test the first char and the last 2 chars of the string to make sure they are ints.
		// if not, try to calculate the value from your starttime and endtime - if one or both of these do not have a value,
		// set totPosTime vars to null and return
		try {
			@SuppressWarnings("unused")
			int testDigit = Integer.parseInt(rawTotPosTime.substring(0, 1));
			testDigit = Integer.parseInt(rawTotPosTime.substring(rawTotPosTime.length()-2));			
		} catch (NumberFormatException e) {
			e.printStackTrace();
		}
		
		// if time String doesn't contain a colon, set value to null and return
		if(rawTotPosTime.charAt(1) != ':' && rawTotPosTime.charAt(2) != ':') {
			this.driveTotPosTimeMin = null;
			this.driveTotPosTimeSec = null;
			return;
		}
		
		// in raw data, when startTime is unknown, python parses this string as "0:00"
		// if this is the case, we need to check to see if it should actually be set to null
		if(rawTotPosTime.equals("0:00")) {
			if(this.driveStartField == null) {
				this.driveTotPosTimeMin = null;
				this.driveTotPosTimeSec = null;
				return;
			}			
		}
		
		// append a leading 0 if time is less than 10 mins
		if(rawTotPosTime.charAt(1) == ':') {
			rawTotPosTime = "0"+rawTotPosTime;
		}
		
		this.driveTotPosTimeMin = Integer.parseInt(rawTotPosTime.substring(0,2));
		this.driveTotPosTimeSec = Integer.parseInt(rawTotPosTime.substring(3,5));
	}	
}