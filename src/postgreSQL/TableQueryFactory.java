package postgreSQL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.io.IOException;
import java.io.FileInputStream;

public class TableQueryFactory {	
	static Connection c;
	
	private static void createConnection() {		
		Properties dbProps = new Properties();
		FileInputStream in;
		try {
			in = new FileInputStream("src/postgreSQL/db.properties");
			dbProps.load(in);
			in.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
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
	}
	
	public static short lookupTeamIDFromName(int year, String teamName) {
		createConnection();
		
		PreparedStatement stmt;
		ResultSet rs;
		short teamID = -1;
		
		try {
			 String sql;			 
			 
			 sql = "SELECT TS.\"TeamSeasonID\"\n"
				 		+ "FROM \"Team_Season\" AS TS\n"
						+ "LEFT OUTER JOIN \"Team_City\" AS TC ON TS.\"TeamCityID\" = TC.\"TeamCityID\"\n"
				 		+ "LEFT OUTER JOIN \"Team_City_Alternate_Names\" AS TCAN ON TC.\"TeamCityID\" = TCAN.\"TeamCityID\"\n"
						+ "WHERE TS.\"SeasonYear\" = ? \n"
						+ "AND (TC.\"TeamCityAcronym\" = ? OR TCAN.\"AlternateTeamAcronym\" = ?)";
			 
			 stmt = c.prepareStatement(sql);
			 stmt.setInt(1, year);
			 stmt.setString(2, teamName.toUpperCase());
			 stmt.setString(3, teamName.toUpperCase());
			 
			 rs = stmt.executeQuery();
			 // need to grab only the first element in the "set"
			 rs.next();
			 teamID = Short.parseShort(rs.getString(1));
			 
			 c.close();
		} catch (SQLException e) {
			e.printStackTrace();
			System.err.println(e.getClass().getName()+ ": "+e.getMessage());
			System.exit(0);
		}
		return teamID;
	}
	
	public static int lookupGameIDFromGameKey(int year, int gameKey) {
		createConnection();
		
		PreparedStatement stmt;
		ResultSet rs;
		int gameID = -1;
		
		try {
			 String sql = "SELECT G.\"GameID\"\n"
				 		+ "FROM \"Game\" AS G\n"						
						+ "WHERE G.\"SeasonYear\" = ? \n"
						+ "AND G.\"GameKey\" = ? ";
			 
			 stmt = c.prepareStatement(sql);
			 stmt.setInt(1, year);
			 stmt.setInt(2, gameKey);
			 
			 rs = stmt.executeQuery();
			 // need to grab only the first element in the "set"
			 rs.next();
			 gameID = Integer.parseInt(rs.getString(1));
			 
			 c.close();
		} catch (SQLException e) {
			e.printStackTrace();
			System.err.println(e.getClass().getName()+ ": "+e.getMessage());
			System.exit(0);
		}
		return gameID;
	}
	
	public static int lookupDriveOutcomeIDFromName(String outcome) {
		createConnection();
		
		PreparedStatement stmt;
		ResultSet rs;
		int driveOutcomeID = -1;
		
		try {
			 String sql = "SELECT DOT.\"DriveOutcomeID\"\n"
				 		+ "FROM \"Drive_Outcome\" AS DOT\n"						
						+ "WHERE DOT.\"DriveOutcomeDesc\" = ? ";						
			 
			 stmt = c.prepareStatement(sql);
			 stmt.setString(1, outcome);
			 
			 rs = stmt.executeQuery();
			 // need to grab only the first element in the "set"
			 rs.next();
			 driveOutcomeID = Integer.parseInt(rs.getString(1));
			 
			 c.close();
		} catch (SQLException e) {
			e.printStackTrace();
			System.err.println(e.getClass().getName()+ ": "+e.getMessage());
			System.err.println("Error encountered when trying to convert outcome type: '" + outcome + "'");
			System.exit(0);
		}
		return driveOutcomeID;
	}	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
}