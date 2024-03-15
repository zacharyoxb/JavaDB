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

    /**
     * Creates the database.
     */
    public static void createDatabase() {
        List<String[]> csvArray = readIntoArray();
        putDataIntoDb(csvArray);
    }

    /**
     * Reads csv file into list full of arrays
     * @return list containing csv values
     */
    public static List<String[]> readIntoArray() {
        String myCsvPath = "38636387.csv";

        List<String[]> csvList = new ArrayList<>();

        try(BufferedReader br = new BufferedReader(new FileReader(myCsvPath))) {
            br.readLine(); // skip attr line
            String currentLine;
            while((currentLine = br.readLine()) != null) {
                String[] currentRow = currentLine.split(",");
                csvList.add(currentRow);
            }
            return csvList;
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Puts the data into the database
     * @param csvList list containing csv data
     */
    public static void putDataIntoDb(List<String[]> csvList) {
        // establish connection
        try(Connection connection = DriverManager.getConnection(url)) {
            // put table DDL statements into string var
            String[] allTables = getAllTables();

            // make statements
            try(Statement statement = connection.createStatement()) {
                statement.executeUpdate("DROP DATABASE IF EXISTS LaLiga");
                statement.executeUpdate("CREATE DATABASE LaLiga");
                statement.executeUpdate("USE LaLiga");
                for(String table : allTables) {
                    statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + table);
                }
            }
            
            // Go through first time: add all teams and their players/sponsors
            for(String[] row : csvList) {
                // add to Referees table
                if(notInTable(connection, "Referees", "Referee_id", Integer.parseInt(row[0]))) {
                    String insertString = "INSERT INTO Referees (Referee_id, Referee_name) VALUES (?, ?)";
                    try(PreparedStatement preparedStatement = connection.prepareStatement(insertString)) {
                        preparedStatement.setInt(1, Integer.parseInt(row[0]));
                        preparedStatement.setString(2, row[1]);
                        preparedStatement.executeUpdate();
                    }
                }
                // add to Teams table
                if(notInTable(connection, "Teams", "Team_name", row[12])) {
                    String insertString = "INSERT INTO Teams (Team_name, Manager, Owner) VALUES (?, ?, ?)";
                    try(PreparedStatement preparedStatement = connection.prepareStatement(insertString)) {
                        preparedStatement.setString(1, row[12]);
                        preparedStatement.setString(2, row[13]);
                        preparedStatement.setString(3, row[14]);
                        preparedStatement.executeUpdate();
                    }
                }
                
                // add to Sponsors table
                if(notInTable(connection, "Sponsors", "Sponsor_id", row[15])) {
                    String insertString = "INSERT INTO Sponsors (Sponsor_id, Sponsor_name) VALUES (?, ?)";
                    try(PreparedStatement preparedStatement = connection.prepareStatement(insertString)) {
                        preparedStatement.setInt(1, Integer.parseInt(row[15]));
                        preparedStatement.setString(2, row[16]);
                        preparedStatement.executeUpdate();
                    }
                }
                // add to SUPPORTS table
                if(notInRelationalTable(connection, "SUPPORTS", "Sponsor_id", "Team_name",
                        row[15], row[12])) {
                    String insertString = "INSERT INTO SUPPORTS (Sponsor_id, Team_name) VALUES (?, ?)";
                    try(PreparedStatement preparedStatement = connection.prepareStatement(insertString)) {
                        preparedStatement.setInt(1, Integer.parseInt(row[15]));
                        preparedStatement.setString(2, row[12]);
                        preparedStatement.executeUpdate();
                    }
                }

                // add to Players table
                if(notInTable(connection, "Players", "Player_id", row[6])) {
                    String insertString = "INSERT INTO Players (Player_id, First_name, Last_name, Age, Position, " +
                            "Nationality) VALUES (?, ?, ?, ?, ?, ?)";
                    try(PreparedStatement preparedStatement = connection.prepareStatement(insertString)) {
                        preparedStatement.setInt(1, Integer.parseInt(row[6]));
                        preparedStatement.setString(2, row[7]);
                        preparedStatement.setString(3, row[8]);
                        preparedStatement.setInt(4, Integer.parseInt(row[9]));
                        preparedStatement.setString(5, row[10]);
                        preparedStatement.setString(6, row[11]);
                        preparedStatement.executeUpdate();
                    }
                }

                // add to PLAYSFOR table
                if(notInRelationalTable(connection, "PLAYSFOR","Player_id","Team_name",
                        row[6], row[12])) {
                    String insertString = "INSERT INTO PLAYSFOR (Player_id, Team_name) VALUES (?, ?)";
                    try(PreparedStatement preparedStatement = connection.prepareStatement(insertString)) {
                        preparedStatement.setInt(1, Integer.parseInt(row[6]));
                        preparedStatement.setString(2, row[12]);
                        preparedStatement.executeUpdate();
                    }
                }

                // add to Games table
                if(notInTable(connection, "Games", "Game_id", row[2])) {
                    String insertString = "INSERT INTO Games (Game_id, Date, Time, Location, Referee_id) VALUES (?, ?, ?, ?, ?)";
                    try(PreparedStatement preparedStatement = connection.prepareStatement(insertString)) {
                        preparedStatement.setInt(1, Integer.parseInt(row[2]));
                        preparedStatement.setString(2, row[3]);
                        preparedStatement.setString(3, row[4]);
                        preparedStatement.setString(4, row[5]);
                        preparedStatement.setString(5, row[0]);
                        preparedStatement.executeUpdate();
                    }
                }

                // add to PLAYS table
                if(notInRelationalTable(connection, "PLAYS", "Team_name", "Game_id",
                        row[12], row[2])) {
                    String insertString = "INSERT INTO PLAYS (Team_name, Game_id) VALUES (?, ?)";
                    try(PreparedStatement preparedStatement = connection.prepareStatement(insertString)) {
                        preparedStatement.setString(1, row[12]);
                        preparedStatement.setInt(2, Integer.parseInt(row[2]));
                        preparedStatement.executeUpdate();
                    }
                }
            }
        } catch(SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void executeQueries() {
        try(Connection connection = DriverManager.getConnection(url)) {
            try(Statement statement = connection.createStatement()) {
                statement.executeQuery("USE LaLiga");
            } catch(SQLException e) {
                throw new RuntimeException();
            }

            try(Statement statement = connection.createStatement()) {
                System.out.println("\nExecuting query:\nDELETE FROM Referees WHERE Referee_id = 4");
                statement.executeQuery("DELETE FROM Referees WHERE Referee_id = 4");
            } catch(SQLException e) {
                System.out.println("Entered catch: DELETE REJECTED");
            }

            try(Statement statement = connection.createStatement()) {
                System.out.println("\nExecuting query:\nDELETE FROM Teams WHERE Team_name = Villareal");
                statement.executeQuery("DELETE FROM Teams WHERE Team_name = Villareal");
            } catch(SQLException e) {
                System.out.println("Entered catch: DELETE REJECTED");
            }

            try(Statement statement = connection.createStatement()) {
                String refQuery = "SELECT Referees.Referee_name, COUNT(Games.Referee_id) AS Games_officiated \n" +
                         "FROM Referees \n" +
                         "JOIN Games ON Referees.Referee_id = Games.Referee_id \n" +
                         "GROUP BY Referees.Referee_name";
                System.out.println("\nExecuting query:\n" + refQuery + "\n");
                ResultSet resultSet = statement.executeQuery(refQuery.replace("\n", ""));

                System.out.printf("%-10s|%-10s", "Referee_name", "Games_officiated\n");
                while(resultSet.next()) {
                    String refName = resultSet.getString("Referee_name");
                    int gamesOfficiated = resultSet.getInt("Games_officiated");
                    System.out.printf("%-10s|%-10s\n", refName, gamesOfficiated);
                }
            } catch(SQLException e) {
                throw new RuntimeException();
            }

            try(Statement statement = connection.createStatement()) {
                String ageQuery = "SELECT Team_name, AVG(Age) AS Average_age \n" +
                "FROM TEAMS\n" +
                "JOIN PLAYSFOR ON TEAMS.Team_name = PLAYSFOR.Team_name\n" +
                "JOIN Players ON PLAYSFOR.Player_id = Players.Player_id\n" +
                "GROUP BY Team_name\n" +
                "HAVING AVG(Age) > 25";
                System.out.println("\nExecuting query:\n" + ageQuery + "\n");
                ResultSet resultSet = statement.executeQuery(ageQuery.replace("\n", ""));

                System.out.printf("%-10s|%-10s", "Team_name", "Average_age\n");
                while(resultSet.next()) {
                    String teamName = resultSet.getString("Team_name");
                    int averageAge = resultSet.getInt("Average_age");
                    System.out.printf("%-10s|%-10s\n", teamName, averageAge);
                }
                
            } catch(SQLException e) {
                throw new RuntimeException();
            }
        } catch(SQLException e) {
            throw new RuntimeException();
        }
    }

    /**
     * Gets all tables in DDL statements
     * @return Array of strings for each table
     */
    private static String[] getAllTables() {
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
                "FOREIGN KEY (Referee_id) REFERENCES Referees(Referee_id) ON DELETE RESTRICT" +
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
                "FOREIGN KEY (Team_name) REFERENCES Teams(Team_name) ON DELETE RESTRICT," +
                "FOREIGN KEY (Game_id) REFERENCES Games(Game_id) ON DELETE CASCADE" +
                ")";

        String playersTable = "Players (" +
                "Player_id INT PRIMARY KEY," +
                "First_name VARCHAR(50) NOT NULL," +
                "Last_name VARCHAR(50) NOT NULL," +
                "Age INT NOT NULL," +
                "Position VARCHAR(50) NOT NULL," +
                "Nationality VARCHAR(50) NOT NULL" +
                ")";

        String playsForTable = "PLAYSFOR (" +
                "Player_id INT PRIMARY KEY," +
                "Team_name VARCHAR(50)," +
                "FOREIGN KEY (Team_name) REFERENCES Teams(Team_name) ON DELETE CASCADE," +
                "FOREIGN KEY (Player_id) REFERENCES Players(Player_id) ON DELETE CASCADE" +
                ")";

        String sponsorsTable = "Sponsors (" +
                "Sponsor_id INT PRIMARY KEY," +
                "Sponsor_name VARCHAR(50) NOT NULL" +
                ")";

        String supportsTable = "SUPPORTS (" +
                "Sponsor_id INT," +
                "Team_name VARCHAR(50)," +
                "PRIMARY KEY (Sponsor_id, Team_name)," +
                "FOREIGN KEY (Sponsor_id) REFERENCES Sponsors(Sponsor_id) ON DELETE CASCADE," +
                "FOREIGN KEY (Team_name) REFERENCES Teams(Team_name) ON DELETE CASCADE" +
                ")";

        return new String[]{refTable, gamesTable, teamsTable, playsTable, playersTable, playsForTable,
                sponsorsTable, supportsTable};
    }

    /**
     * Checks if a value already exists in a table.
     * @param connection sql connection to use
     * @param tableName name of table to check
     * @param columnName name of column to check
     * @param columnValue value to check
     * @return true if not in table, false if it is
     */
    public static <T> boolean notInTable(Connection connection, String tableName, String columnName, T columnValue) {
        String sql = String.format("SELECT * FROM %s WHERE %s = ?", tableName, columnName);

        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            // type check
            if(columnValue.getClass() == Integer.class) {
                preparedStatement.setInt(1, (Integer) columnValue);
            } else if(columnValue.getClass() == String.class) {
                preparedStatement.setString(1, (String) columnValue);
            }

            try(ResultSet resultSet = preparedStatement.executeQuery()) {
                return !resultSet.next();
            }
        } catch(SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Checks if a composite key set already exists in a relational table.
     * @param connection sql connection to use
     * @param tableName name of table to check
     * @param keyName1 name of first key to check
     * @param keyName2 name of second key to check
     * @param keyValue1 first value to check
     * @param keyValue2 second value to check
     * @return true if not in relational table, false if it is
     */
    public static <T> boolean notInRelationalTable(Connection connection, String tableName, String keyName1,
                                                   String keyName2, T keyValue1, T keyValue2) {
        String sql = String.format("SELECT * FROM %s WHERE %s = ? AND %s = ?", tableName, keyName1, keyName2);

        try(PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            // key 1
            if(keyValue1.getClass() == Integer.class) {
                preparedStatement.setInt(1, (Integer) keyValue1);
            } else if(keyValue1.getClass() == String.class) {
                preparedStatement.setString(1, (String) keyValue1);
            }

            // key 2
            if(keyValue2.getClass() == Integer.class) {
                preparedStatement.setInt(2, (Integer) keyValue2);
            } else if(keyValue2.getClass() == String.class) {
                preparedStatement.setString(2, (String) keyValue2);
            }

            try(ResultSet resultSet = preparedStatement.executeQuery()) {
                return !resultSet.next();
            }
        } catch(SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        createDatabase();
        executeQueries();
    }
}
