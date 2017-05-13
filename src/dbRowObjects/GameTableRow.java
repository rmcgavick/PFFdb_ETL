package dbRowObjects;


public class GameTableRow {

	public int GameKey;
	public int SeasonYear;
	public int Week;
	public int Month;
	public int Day;
	public String WeekDay;
	public String GameStartTime;
	public Integer ForeignCityID;
	public int HomeTeamID, AwayTeamID;
	public int HomeTeamFirstDowns, AwayTeamFirstDowns;
	public int HomeTeamTotYds, AwayTeamTotYds;
	public int HomeTeamPassYds, AwayTeamPassYds;
	public int HomeTeamRushYds, AwayTeamRushYds;
	public int HomeTeamTurnovers, AwayTeamTurnovers;
	public int HomeTeamPenaltyCnt, AwayTeamPenaltyCnt;
	public int HomeTeamPenaltyYds, AwayTeamPenaltyYds;
	public int HomeTeamPuntCnt, AwayTeamPuntCnt;
	public int HomeTeamPuntYds, AwayTeamPuntYds;
	public int HomeTeamPuntAvg, AwayTeamPuntAvg;
	public int HomeTeamFinalScore, AwayTeamFinalScore;
		
	// need the year argument because the schedule info changes from year 2014 to 2015 - (the 'meridiem' (AM/PM) arg is only in year 2015 and 2016)
	public void populateScheduleData(String rawMonth, String rawDay, String rawWeekDay, String rawStartTime, String rawMeridiem) {
		/*
		 * parse schedule info from rawData string: need Month, Day, WeekDay, & GameStartTime
		 * Example of 2014 and before data:
		 * {u'week': 17, u'gamekey': u'56425', u'season_type': u'REG', u'away': u'STL', u'year': 2014, u'month': 12, u'eid': u'2014122815', u'time': u'4:25', u'home': u'SEA', u'wday': u'Sun', u'day': 28}
		 *
		 * Example of 2015 and after data:
		 * {u'week': 1, u'meridiem': u'PM', u'gamekey': u'56503', u'season_type': u'REG', u'away': u'PIT', u'year': 2015, u'month': 9, u'eid': u'2015091000', u'time': u'8:30', u'home': u'NE', u'wday': u'Thu', u'day': 10}
		 */

		rawMonth = rawMonth.substring(rawMonth.length()-2,rawMonth.length());		
		this.Month = Integer.parseInt(rawMonth.trim());
		
		rawDay = rawDay.substring(rawDay.length()-4,rawDay.length()-2);
		this.Day = Integer.parseInt(rawDay.trim());
		
		rawWeekDay = rawWeekDay.substring(rawWeekDay.length()-4,rawWeekDay.length()-1);
		this.WeekDay = rawWeekDay;
		
		rawStartTime = rawStartTime.substring(rawStartTime.length()-5,rawStartTime.length()-1);
		if(rawStartTime.charAt(0) == '\'') {
			rawStartTime = rawStartTime.substring(1,rawStartTime.length());
		}
		
		if(rawMeridiem != null) {
			rawMeridiem = rawMeridiem.substring(rawMeridiem.length()-3,rawMeridiem.length()-1);
			
			if(rawMeridiem.equals("PM")) {
				String[] timeParts = rawStartTime.split(":");
				int hour = Integer.parseInt(timeParts[0]);
				hour += 12;
				rawStartTime = hour + ":" + timeParts[1];
			}
		}
		
		this.GameStartTime = rawStartTime;
	}
	
	// need the year to determine which entry in Team_Season you should use
	public void populateHomeTeamData(String rawHomeTeamFirstDowns, String rawHomeTeamTotYds, String rawHomeTeamPassYds,
			String rawHomeTeamRushYds, String rawHomeTeamTurnovers, String rawHomeTeamPenaltyCnt, String rawHomeTeamPenaltyYds,
			String rawHomeTeamPuntCnt, String rawHomeTeamPuntYds, String rawHomeTeamPuntAvg) {
		/*
		 * parse home team agg data from rawData string - querying Team_Season data to get the appropriate HomeTeamID
		 */
		String digits = "0123456789";
		
		rawHomeTeamFirstDowns = rawHomeTeamFirstDowns.substring(rawHomeTeamFirstDowns.length()-2,rawHomeTeamFirstDowns.length());
		if(digits.indexOf(rawHomeTeamFirstDowns.charAt(0)) == -1) {
			rawHomeTeamFirstDowns = rawHomeTeamFirstDowns.substring(1);
		}
		this.HomeTeamFirstDowns = Integer.parseInt(rawHomeTeamFirstDowns);
		
		rawHomeTeamTotYds = rawHomeTeamTotYds.substring(rawHomeTeamTotYds.length()-3,rawHomeTeamTotYds.length());
		if(digits.indexOf(rawHomeTeamTotYds.charAt(0)) == -1) {
			rawHomeTeamTotYds = rawHomeTeamTotYds.substring(1);
		}
		if(digits.indexOf(rawHomeTeamTotYds.charAt(0)) == -1) {
			rawHomeTeamTotYds = rawHomeTeamTotYds.substring(1);
		}
		this.HomeTeamTotYds = Integer.parseInt(rawHomeTeamTotYds);
		
		rawHomeTeamPassYds = rawHomeTeamPassYds.substring(rawHomeTeamPassYds.length()-3,rawHomeTeamPassYds.length());
		if(digits.indexOf(rawHomeTeamPassYds.charAt(0)) == -1) {
			rawHomeTeamPassYds = rawHomeTeamPassYds.substring(1);
		}
		if(digits.indexOf(rawHomeTeamPassYds.charAt(0)) == -1) {
			rawHomeTeamPassYds = rawHomeTeamPassYds.substring(1);
		}
		this.HomeTeamPassYds = Integer.parseInt(rawHomeTeamPassYds);
		
		rawHomeTeamRushYds = rawHomeTeamRushYds.substring(rawHomeTeamRushYds.length()-3,rawHomeTeamRushYds.length());
		if(digits.indexOf(rawHomeTeamRushYds.charAt(0)) == -1) {
			rawHomeTeamRushYds = rawHomeTeamRushYds.substring(1);
		}
		if(digits.indexOf(rawHomeTeamRushYds.charAt(0)) == -1) {
			rawHomeTeamRushYds = rawHomeTeamRushYds.substring(1);
		}
		this.HomeTeamRushYds = Integer.parseInt(rawHomeTeamRushYds);
		
		rawHomeTeamTurnovers = rawHomeTeamTurnovers.substring(rawHomeTeamTurnovers.length()-2,rawHomeTeamTurnovers.length());
		if(digits.indexOf(rawHomeTeamTurnovers.charAt(0)) == -1) {
			rawHomeTeamTurnovers = rawHomeTeamTurnovers.substring(1);
		}
		this.HomeTeamTurnovers = Integer.parseInt(rawHomeTeamTurnovers);
		
		rawHomeTeamPenaltyCnt = rawHomeTeamPenaltyCnt.substring(rawHomeTeamPenaltyCnt.length()-2,rawHomeTeamPenaltyCnt.length());
		if(digits.indexOf(rawHomeTeamPenaltyCnt.charAt(0)) == -1) {
			rawHomeTeamPenaltyCnt = rawHomeTeamPenaltyCnt.substring(1);
		}
		this.HomeTeamPenaltyCnt = Integer.parseInt(rawHomeTeamPenaltyCnt);
		
		rawHomeTeamPenaltyYds = rawHomeTeamPenaltyYds.substring(rawHomeTeamPenaltyYds.length()-3,rawHomeTeamPenaltyYds.length());
		if(digits.indexOf(rawHomeTeamPenaltyYds.charAt(0)) == -1) {
			rawHomeTeamPenaltyYds = rawHomeTeamPenaltyYds.substring(1);
		}
		if(digits.indexOf(rawHomeTeamPenaltyYds.charAt(0)) == -1) {
			rawHomeTeamPenaltyYds = rawHomeTeamPenaltyYds.substring(1);
		}
		this.HomeTeamPenaltyYds = Integer.parseInt(rawHomeTeamPenaltyYds);
		
		rawHomeTeamPuntCnt = rawHomeTeamPuntCnt.substring(rawHomeTeamPuntCnt.length()-2,rawHomeTeamPuntCnt.length());
		if(digits.indexOf(rawHomeTeamPuntCnt.charAt(0)) == -1) {
			rawHomeTeamPuntCnt = rawHomeTeamPuntCnt.substring(1);
		}
		this.HomeTeamPuntCnt = Integer.parseInt(rawHomeTeamPuntCnt);
		
		rawHomeTeamPuntYds = rawHomeTeamPuntYds.substring(rawHomeTeamPuntYds.length()-3,rawHomeTeamPuntYds.length());
		if(digits.indexOf(rawHomeTeamPuntYds.charAt(0)) == -1) {
			rawHomeTeamPuntYds = rawHomeTeamPuntYds.substring(1);
		}
		if(digits.indexOf(rawHomeTeamPuntYds.charAt(0)) == -1) {
			rawHomeTeamPuntYds = rawHomeTeamPuntYds.substring(1);
		}
		this.HomeTeamPuntYds = Integer.parseInt(rawHomeTeamPuntYds);
		
		rawHomeTeamPuntAvg = rawHomeTeamPuntAvg.substring(rawHomeTeamPuntAvg.length()-2,rawHomeTeamPuntAvg.length());
		if(digits.indexOf(rawHomeTeamPuntAvg.charAt(0)) == -1) {
			rawHomeTeamPuntAvg = rawHomeTeamPuntAvg.substring(1);
		}
		this.HomeTeamPuntAvg = Integer.parseInt(rawHomeTeamPuntAvg);
	}
	
	// need the year to determine which entry in Team_Season you should use	
	public void populateAwayTeamData(String rawAwayTeamFirstDowns, String rawAwayTeamTotYds, String rawAwayTeamPassYds,
			String rawAwayTeamRushYds, String rawAwayTeamTurnovers, String rawAwayTeamPenaltyCnt, String rawAwayTeamPenaltyYds,
			String rawAwayTeamPuntCnt, String rawAwayTeamPuntYds, String rawAwayTeamPuntAvg) {
		/*
		 * parse away team agg data from rawData string - querying Team_Season data to get the appropriate AwayTeamID
		 */
		String digits = "0123456789";
		
		rawAwayTeamFirstDowns = rawAwayTeamFirstDowns.substring(rawAwayTeamFirstDowns.length()-2,rawAwayTeamFirstDowns.length());
		if(digits.indexOf(rawAwayTeamFirstDowns.charAt(0)) == -1) {
			rawAwayTeamFirstDowns = rawAwayTeamFirstDowns.substring(1);
		}
		this.HomeTeamFirstDowns = Integer.parseInt(rawAwayTeamFirstDowns);
		
		rawAwayTeamTotYds = rawAwayTeamTotYds.substring(rawAwayTeamTotYds.length()-3,rawAwayTeamTotYds.length());
		if(digits.indexOf(rawAwayTeamTotYds.charAt(0)) == -1) {
			rawAwayTeamTotYds = rawAwayTeamTotYds.substring(1);
		}
		if(digits.indexOf(rawAwayTeamTotYds.charAt(0)) == -1) {
			rawAwayTeamTotYds = rawAwayTeamTotYds.substring(1);
		}
		this.HomeTeamTotYds = Integer.parseInt(rawAwayTeamTotYds);
		
		rawAwayTeamPassYds = rawAwayTeamPassYds.substring(rawAwayTeamPassYds.length()-3,rawAwayTeamPassYds.length());
		if(digits.indexOf(rawAwayTeamPassYds.charAt(0)) == -1) {
			rawAwayTeamPassYds = rawAwayTeamPassYds.substring(1);
		}
		if(digits.indexOf(rawAwayTeamPassYds.charAt(0)) == -1) {
			rawAwayTeamPassYds = rawAwayTeamPassYds.substring(1);
		}
		this.HomeTeamPassYds = Integer.parseInt(rawAwayTeamPassYds);
		
		rawAwayTeamRushYds = rawAwayTeamRushYds.substring(rawAwayTeamRushYds.length()-3,rawAwayTeamRushYds.length());
		if(digits.indexOf(rawAwayTeamRushYds.charAt(0)) == -1) {
			rawAwayTeamRushYds = rawAwayTeamRushYds.substring(1);
		}
		if(digits.indexOf(rawAwayTeamRushYds.charAt(0)) == -1) {
			rawAwayTeamRushYds = rawAwayTeamRushYds.substring(1);
		}
		this.HomeTeamRushYds = Integer.parseInt(rawAwayTeamRushYds);
		
		rawAwayTeamTurnovers = rawAwayTeamTurnovers.substring(rawAwayTeamTurnovers.length()-2,rawAwayTeamTurnovers.length());
		if(digits.indexOf(rawAwayTeamTurnovers.charAt(0)) == -1) {
			rawAwayTeamTurnovers = rawAwayTeamTurnovers.substring(1);
		}
		this.HomeTeamTurnovers = Integer.parseInt(rawAwayTeamTurnovers);
		
		rawAwayTeamPenaltyCnt = rawAwayTeamPenaltyCnt.substring(rawAwayTeamPenaltyCnt.length()-2,rawAwayTeamPenaltyCnt.length());
		if(digits.indexOf(rawAwayTeamPenaltyCnt.charAt(0)) == -1) {
			rawAwayTeamPenaltyCnt = rawAwayTeamPenaltyCnt.substring(1);
		}
		this.HomeTeamPenaltyCnt = Integer.parseInt(rawAwayTeamPenaltyCnt);
		
		rawAwayTeamPenaltyYds = rawAwayTeamPenaltyYds.substring(rawAwayTeamPenaltyYds.length()-3,rawAwayTeamPenaltyYds.length());
		if(digits.indexOf(rawAwayTeamPenaltyYds.charAt(0)) == -1) {
			rawAwayTeamPenaltyYds = rawAwayTeamPenaltyYds.substring(1);
		}
		if(digits.indexOf(rawAwayTeamPenaltyYds.charAt(0)) == -1) {
			rawAwayTeamPenaltyYds = rawAwayTeamPenaltyYds.substring(1);
		}
		this.HomeTeamPenaltyYds = Integer.parseInt(rawAwayTeamPenaltyYds);
		
		rawAwayTeamPuntCnt = rawAwayTeamPuntCnt.substring(rawAwayTeamPuntCnt.length()-2,rawAwayTeamPuntCnt.length());
		if(digits.indexOf(rawAwayTeamPuntCnt.charAt(0)) == -1) {
			rawAwayTeamPuntCnt = rawAwayTeamPuntCnt.substring(1);
		}
		this.HomeTeamPuntCnt = Integer.parseInt(rawAwayTeamPuntCnt);
		
		rawAwayTeamPuntYds = rawAwayTeamPuntYds.substring(rawAwayTeamPuntYds.length()-3,rawAwayTeamPuntYds.length());
		if(digits.indexOf(rawAwayTeamPuntYds.charAt(0)) == -1) {
			rawAwayTeamPuntYds = rawAwayTeamPuntYds.substring(1);
		}
		if(digits.indexOf(rawAwayTeamPuntYds.charAt(0)) == -1) {
			rawAwayTeamPuntYds = rawAwayTeamPuntYds.substring(1);
		}
		this.HomeTeamPuntYds = Integer.parseInt(rawAwayTeamPuntYds);
		
		rawAwayTeamPuntAvg = rawAwayTeamPuntAvg.substring(rawAwayTeamPuntAvg.length()-2,rawAwayTeamPuntAvg.length());
		if(digits.indexOf(rawAwayTeamPuntAvg.charAt(0)) == -1) {
			rawAwayTeamPuntAvg = rawAwayTeamPuntAvg.substring(1);
		}
		this.HomeTeamPuntAvg = Integer.parseInt(rawAwayTeamPuntAvg);
	}
	
	public static void main(String[] args) {
		
	}
}