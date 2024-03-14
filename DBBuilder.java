import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class DBBuilder {
    final static String url = "jdbc:mysql://localhost:3306/";

    public static void createDatabase() {
        String createReferees = "";
        String createGames = "";
        String createTeams = "";
        String createPlayers = "";
        String createSponsors = "";
    }

    public List<String[]> readIntoArray() {
        String myCsvPath = "38636387.csv";

        List<String[]> csvList = new ArrayList<>();

        try(BufferedReader br = new BufferedReader(new FileReader(myCsvPath))) {
            String currentLine;
            while((currentLine = br.readLine()) != null) {
                String[] currentRow = currentLine.split(",");
                csvList.add(currentRow);
            }
            return csvList;
        } catch(IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void putDataIntoDb(List<String[]> csvList) {
        // establish connection
        try(Connection connection = DriverManager.getConnection(url)) {
            // put table DDL statements into string var
            String refTable = "Referees (" +
                    "Referee_id INT PRIMARY KEY," +
                    "Referee_name VARCHAR(50) NOT NULL" +
                    ")";

            String gamesTable = "Games (" +
                    "Game_id INT PRIMARY KEY," +
                    "Date VARCHAR(10) NOT NULL," +
                    "Time VARCHAR(10) NOT NULL," +
                    "Location VARCHAR(50) NOT NULL," +
                    "Referee_id INT NOT NULL," +
                    "FOREIGN KEY (Referee_id) REFERENCES Referees(Referee_id) ON DELETE REJECT" +
                    ")";

            String teamsTable = "Teams (" +
                    "Team_name VARCHAR(50) PRIMARY KEY," +
                    "Manager VARCHAR(50) NOT NULL," +
                    "Owner VARCHAR (50) NOT NULL" +
                    ")";

            String playsTable = "PLAYS (" +
                    "Team_name VARCHAR(50)," +
                    "Game_id INT," +
                    "PRIMARY KEY (Team_name, Game_id)," +
                    "FOREIGN KEY (Team_name REFERENCES Teams(Team_name) ON DELETE REJECT," +
                    "FOREIGN KEY (Game_id) REFERENCES Games(Game_id) ON DELETE CASCADE" +
                    ")";

            String playersTable = "Players (" +
                    "Player_id INT PRIMARY KEY," +
                    "First_name VARCHAR(50) NOT NULL," +
                    "Last_name VARCHAR(50) NOT NULL," +
                    "Age INT NOT NULL," +
                    "Position VARCHAR(50) NOT NULL," +
                    "Nationality VARCHAR(50) NOT NULL," +
                    ")";

            String playsForTable = "PLAYSFOR (" +
                    "Team_name VARCHAR(50) PRIMARY KEY," +
                    "Player_id INT," +
                    "FOREIGN KEY (Team_name) REFERENCES Teams(Team_name) ON DELETE CASCADE," +
                    "FOREIGN KEY (Player_id) REFERENCES Players(Player_id) ON DELETE CASCADE" +
                    ")";

            String sponsorsTable = "Sponsors (" +
                    "Sponsor_id INT PRIMARY KEY," +
                    "Sponsor_name VARCHAR(50) NOT NULL" +
                    ")";

            String supportsTable = "SUPPORTS (" +
                    "Sponsor_id INT," +
                    "Team_id INT," +
                    "PRIMARY KEY (Sponsor_id, Team_id)," +
                    "FOREIGN KEY (Sponsor_id) REFERENCES Sponsors(Sponsor_id) ON DELETE CASCADE," +
                    "FOREIGN KEY (Team_id) REFERENCES Teams(Team_id) ON DELETE CASCADE" +
                    ")";

            String[] allTables = {refTable, gamesTable, teamsTable, playsTable, playersTable, playsForTable,
                    sponsorsTable, supportsTable};

            // make statements
            try(Statement statement = connection.createStatement()) {
                statement.executeUpdate("CREATE DATABASE IF NOT EXISTS LaLiga");
                statement.executeUpdate("USE LaLiga");
                for(String table : allTables) {
                    statement.executeUpdate("CREATE TABLE IF NOT EXISTS "+ table);
                }
            }
        } catch(SQLException e) {
            e.printStackTrace();
        }
    }
}
