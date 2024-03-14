import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class DBBuilder {
    final static String url = "jdbc:mysql://localhost:3306/";

    public static void createDatabase() {
        List<String[]> csvArray = readIntoArray();
        putDataIntoDb(csvArray);
    }

    public static List<String[]> readIntoArray() {
        String myCsvPath = "38636387.csv";

        List<String[]> csvList = new ArrayList<>();

        try(BufferedReader br = new BufferedReader(new FileReader(myCsvPath))) {
            String currentLine;
            br.lines().skip(1);
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

    public static void putDataIntoDb(List<String[]> csvList) {
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
            
            // Go through first time: add all teams and their players/sponsors
            for(String[] row : csvList) {
                // check if ref entry already exists: if not, add it
                if(!isInTable("Referee", "Referee_id", Integer.parseInt(row[0]))) {
                    String insertString = "INSERT INTO Referee (Referee_id, Referee_name) VALUES (?, ?)";
                    try(PreparedStatement preparedStatement = connection.prepareStatement(insertString)) {
                        preparedStatement.setInt(1, Integer.parseInt(row[0]));
                        preparedStatement.setString(2, row[1]);
                    }
                }

            }
        } catch(SQLException e) {
            e.printStackTrace();
        }

        
    }
    /**
     * Checks if a value already exists in a table.
     * @param <T> ambiguous type
     * @param tableName name of table to check
     * @param column_name name of column to check
     * @param column_value name of value to check
     * @return true if already exists, false if not
     */
    public static <T> boolean isInTable(String tableName, String column_name, T column_value) {
        String sql = String.format("SELECT * FROM %s WHERE %s = ?", tableName, column_name);

        try(Connection connection = DriverManager.getConnection(url)) {
            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                // type check
                if(column_value.getClass() == Integer.class) {
                    preparedStatement.setInt(1, (Integer) column_value);
                } else if(column_value.getClass() == String.class) {
                    preparedStatement.setString(1, (String) column_value);
                }

                try(ResultSet resultSet = preparedStatement.executeQuery()) {
                    if(resultSet.next()) {
                        return true;
                    }
                    else {
                        return false;
                    }
                }
            } catch(SQLException e) {
                e.printStackTrace();
            }
        } catch(SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static void main(String[] args) {
        createDatabase();
    }
}
