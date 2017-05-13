package etl;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

public class PlayerETL {			
	
	public static LinkedBlockingQueue<String[]> getTempTableData()  {
		String csvFilePath;
		String delimiter = ",";
		LinkedBlockingQueue<String[]> q = new LinkedBlockingQueue<String[]>();
		
		for(int year = 2009; year<2017; year++) {
			csvFilePath = "/C:/users/Riley/workspace/PFFDB_Data_Load/dataimport/"+ year +"AllPlayerData.csv";
		
			try {
				readFromFile(q, csvFilePath, delimiter);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return q;
	}
		
	private static void readFromFile(LinkedBlockingQueue<String[]> Q, String path, String delimiter) throws IOException {		
		BufferedReader br = new BufferedReader(new FileReader(path));		
		
		String currLine;
		String[] inputData;		
		String regex = "'";
		String newStr = "";
		
		while((currLine = br.readLine()) != null) {			
			// split currLine using regex
			inputData = currLine.trim().split(delimiter);
			// each line, check for and escape any single-quotes
			for(int i=0; i<15; i++) {
				if(inputData[i].contains("'")) {
					newStr = inputData[i].replaceAll(regex, "''");
					inputData[i] = newStr;
				}
			}
			Q.add(inputData);
		}		
		br.close();
	}
	
	public static void main(String[] args) {
		
		
	}

}
