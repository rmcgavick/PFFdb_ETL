package web;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Calendar;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Properties;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class GetPlayerPositionDataFromWeb {
	
	public static ConcurrentHashMap<String,String> getAllPlayersByNameSinceYear(int year) {
		Properties webProps = new Properties();
		
		try {
			FileInputStream in = new FileInputStream("src/web/web.properties");
			webProps.load(in);
			in.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
			System.exit(-1);
		}
		
		String playerRegistryBaseURL = webProps.getProperty("footballDBbaseURL");
		String playerRegistryURL;
		Document doc;		
		ConcurrentHashMap<String,String> allPlayerDataInRange = new ConcurrentHashMap<String,String>();		
		
		for(char c = 'A'; c <= 'Z'; c++) {
			int pageNum = 1;
			// increment through each AVAILABLE page for players with last name of 'c', until the document parse throws an error
			while(true) {
				playerRegistryURL = playerRegistryBaseURL + pageNum + "&letter=" +c;
				
				// will parse pages until we reach the end of pages with the first letter of players' last name
				try {
					doc = getDocumentFromURL(playerRegistryURL);
					if(doc.getElementsByClass("row0").isEmpty()) {
						System.out.println("Got all "+ pageNum +" pages of data for players with last name of "+ c);
						break;
					}
					// add all data from the current page (doc) to our HashMap
					allPlayerDataInRange = getAllPlayerDataFromDocumentInYearRangePresentToN(doc,year,allPlayerDataInRange);

				} catch (IOException e) {
					e.printStackTrace();
				}
				pageNum++;
			}			
		}
		System.out.println("Success! Parsed all player data from A to Z");
		
		return allPlayerDataInRange;
	}

	private static Document getDocumentFromURL(String url) throws IOException {
		return Jsoup.connect(url).get();
	}
	
	private static ConcurrentHashMap<String, String> getAllPlayerDataFromDocumentInYearRangePresentToN(Document doc, int year, ConcurrentHashMap<String,String> map) {
		// first we need to access all the rows in the doc arg ("statistics" table)
		Element playerTable = doc.select("table").get(0);
		Elements rows = playerTable.select("tr");
		
		// then, for each row in the table, check the last (3rd) column "Team" - will need to parse this string for any year values.
		for(int i=1; i < rows.size(); i++) { 								// start at 1 because the first row is headers
			Element row = rows.get(i);
			Elements cols = row.select("td");
			// first check cols[3] (teams) - and parse the year			
			Element currColumn = cols.get(3);
			
			// if we find a valid year value that is NOT in our range, we can skip this row completely
			if(isYearInRangeOrEmpty(currColumn, year)) {
				// now process the position (1st column); We're only interested in positions containing "QB", "RB", "WR", "TE", or "K"
				currColumn = cols.get(1);
				String[] validPositions = {"QB","RB","WR","TE","K"};
				
				for(int j=0; j<validPositions.length; j++) {
					if(currColumn.text().contains(validPositions[j])) {
						//if we get here, we found a position we are interested in: get the name from the 0th column
						currColumn = cols.get(0);
						String playerFullName = currColumn.getElementsByTag("a").text();
						
						// add this playername and position pair to the hashmap
						map.put(playerFullName, validPositions[j]);
						
						// finally, break out of inner for loop (positions); this will also go to the next iteration of the outer for loop (years)
						break;
					}
				}
			}
		}
		return map;
	}
	
	private static boolean isYearInRangeOrEmpty(Element td, int year) {
		String tdStr = td.text();
		int currYear = Calendar.getInstance().get(Calendar.YEAR);		
		
		// first check if any specific year between 'year' and currYear are found. if so, return true
		for(int i = year; i<= currYear; i++) {
			String yearStr = i + "";
			if(tdStr.contains(yearStr)) {
				return true;
			}
		}

		// next, check to see if the string even contains any digits. If it does, but it obviously doesn't contain the specific year
		// in range because it passed the check above, return false;
		String[] digits = {"0","1","2","3","4","5","6","7","8","9"};
		for(int i=0; i<digits.length; i++) {
			if(tdStr.contains(digits[i])) {
				return false;
			}
		}
		
		// if we made it here - we know the string doesn't have any valid digits (no year value at all);
		// add it to our set
		return true;
	}
	
	public static void main(String[] args) {		
		//getAllPlayersByNameSinceYear(cutoffYearForHistoricalData);
	}
}