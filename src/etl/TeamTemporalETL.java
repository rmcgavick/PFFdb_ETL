package etl;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class TeamTemporalETL {
	
	static String csvFilePath = "/C:/users/Riley/workspace/PFFDB_Data_Load/dataimport/TeamSeasonRawData.csv";
	static String delimiter = ",";
	static Queue<String[]> q;
	
	public static LinkedBlockingQueue<int[]> getTableData()  {
		try {
			q = readFromFile(csvFilePath, delimiter);
		} catch (IOException e) {
			e.printStackTrace();
		}				
		
		// variable to hold returned results
	    LinkedBlockingQueue<int[]> teamSeasonTableResults = new LinkedBlockingQueue<int[]>();
		// method variables
        @SuppressWarnings("unused")
		String currLine[], prevLine[];
        int currTeamID, prevTeamID=0;
        LinkedList<String[]> currentTeamList = new LinkedList<String[]>();
       
        while(!q.isEmpty()) {
            currLine = q.poll();
            currTeamID = Integer.parseInt(currLine[0]);
            
            if(prevTeamID == 0)
                prevTeamID = currTeamID; 

            // we reached a new Team historical section in the queue OR reached end of list
            if(currTeamID != prevTeamID || q.isEmpty()) {
                teamSeasonTableResults = addTeamListToResults(currentTeamList, teamSeasonTableResults);
               
                currentTeamList = new LinkedList<String[]>();
                currentTeamList.add(currLine);
            }

            // still the same team
            if(currTeamID == prevTeamID) {
                currentTeamList.add(currLine);
            }
            
            prevLine = currLine;
            prevTeamID = currTeamID;                        
        }
        
        //teamTemporalTableResults is populated at this point
        // do something with the data
        //int count = 1;
        //for(int[] arr : teamSeasonTableResults) {        	
        //	System.out.println("Row " + count++ + ": " + Arrays.toString(arr));
        //}
        return teamSeasonTableResults;
	}
	
	private static LinkedBlockingQueue<int[]> addTeamListToResults(LinkedList<String[]> currTeamList, LinkedBlockingQueue<int[]> masterList) {
        int teamID, currYear=0, divConfID, currCoachTypeID, currCoachID, HC_ID=0, OC_ID=0, DC_ID=0;
        String[] currListItem;
       
        //team ID and divConfID won't change in the scope of this function
        teamID = Integer.parseInt(currTeamList.peek()[0].trim());
        divConfID = getDivConfID(teamID);
        
        while(!currTeamList.isEmpty()) {
            currListItem = currTeamList.poll();
            
            // set current year on first iteration
            if(currYear == 0) {
                currYear = convertToYear(currListItem[1]);
            }
           
            // if the input list skips years, or we just incremented to the next year, add everything to return list
            if(currYear < convertToYear(currListItem[1])) {
                for(int i=currYear; i< convertToYear(currListItem[1]); i++) {
                    int[] tempListItem = {teamID,i,divConfID,HC_ID,OC_ID,DC_ID};
                    masterList.add(tempListItem);
                }
                currYear = convertToYear(currListItem[1]);
            }
           
            currCoachTypeID = Integer.parseInt(currListItem[3]);
            currCoachID = Integer.parseInt(currListItem[2]);
           
            // check coachIDs on each iteration
            if(currCoachTypeID == 1 || currCoachTypeID > 3) {
                HC_ID = currCoachID;
            }
            if(currCoachTypeID == 2 || currCoachTypeID == 4) {
                OC_ID = currCoachID;
            }
            if(currCoachTypeID == 3 || currCoachTypeID == 5) {
                DC_ID = currCoachID;
            }
           
            // check for last entry in this list
            if(currTeamList.isEmpty()) {
            	int lastTeamYearInCity = 2016;
            	// handle the only case in our data from 2009-2016 where a team DIDN'T finish their season at their home city in 2016 (STL Rams == 34)
            	if(teamID == 34)
            		lastTeamYearInCity = 2015;
            	
            	for(int i=currYear; i<=lastTeamYearInCity; i++) {
                    int[] tempListItem = {teamID,i,divConfID,HC_ID,OC_ID,DC_ID};
                    masterList.add(tempListItem);	
            		
            	}            
            }
        }
        return masterList;
    }
	
	private static Queue<String[]> readFromFile(String path, String delimiter) throws IOException {		
		BufferedReader br = new BufferedReader(new FileReader(path));
		
		Queue<String[]> tempQ = new LinkedList<String[]>();
		
		String currLine;
		String[] inputData;		
		
		while((currLine = br.readLine()) != null) {			
			// split currLine using regex
			inputData = currLine.trim().split(delimiter);
			tempQ.add(inputData);
		}
		
		br.close();

		return tempQ;
	}
	
	// hard-coded mappings from division ID to conference ID
	private static int getDivConfID(int teamID) {
		int divConfID = 0;
		if(teamID == 7 || teamID == 11 || teamID == 17 || teamID == 26)
			divConfID = 1;
		if(teamID == 9 || teamID == 10 || teamID == 21 || teamID == 22)
			divConfID = 2;
		if(teamID == 5 || teamID == 16 || teamID == 23 || teamID == 24)
			divConfID = 3;
		if(teamID == 1 || teamID == 25 || teamID == 30 || teamID == 27 || teamID == 33)
			divConfID = 4;
		if(teamID == 8 || teamID == 12 || teamID == 29 || teamID == 32)
			divConfID = 5;
		if(teamID == 2 || teamID == 6 || teamID == 14 || teamID == 20)
			divConfID = 6;
		if(teamID == 3 || teamID == 18 || teamID == 19 || teamID == 28)
			divConfID = 7;
		if(teamID == 4 || teamID == 13 || teamID == 15 || teamID == 31 || teamID == 34)
			divConfID = 8;

		return divConfID;
	}
	
	private static int convertToYear(String partialYearStr) {
        String returnYear = "20";
       
        if(partialYearStr.equals("9"))
            returnYear += "09";
        else
            returnYear += partialYearStr;
       
        return Integer.parseInt(returnYear);
    }

	public static void main(String[] args) {
		//LinkedBlockingQueue<int[]> test = getTableData();
		
	}
}
