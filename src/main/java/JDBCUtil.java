import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class JDBCUtil {
    String URL, user, password;
    Connection connection;
    public JDBCUtil(String user, String password, String DBName) {
        this.URL = "jdbc:postgresql://localhost:5432/" + DBName;
        this.user = user;
        this.password = password;
        this.connection = null;
    }

    public void getConnection() {
        // Load the driver class
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException ex) {
            System.out.println("Unable to load the class. Terminating the program");
            ex.printStackTrace();
        }
        // Get the connection
        try {
            // Set postgres properties
            Properties props = new Properties();
            props.setProperty("user", user);
            props.setProperty("password", password);
            connection = DriverManager.getConnection(URL, props);
        } catch (SQLException ex) {
            System.out.println("Error getting connection: " + ex.getMessage());
        } catch (Exception ex) {
            System.out.println("Error: " + ex.getMessage());
        }
    }

    public void printResultSet(ResultSet rs, int limit) throws SQLException {
        ResultSetMetaData rsData = rs.getMetaData();
        int columnsNumber = rsData.getColumnCount();
        List<String>[] columns = new ArrayList[columnsNumber + 1];
        for (int i = 0; i <= columnsNumber; i++) {
            columns[i] = new ArrayList<>();
        }

        int[] lengths = new int[columnsNumber + 1];
        columns[0].add("#");
        for (int i = 1; i <= columnsNumber; i++) {
            String columnName = rsData.getColumnName(i);
            columns[i].add(columnName);
            lengths[i] = columnName.length();
        }

        int count = 1;
        while (rs.next()) {
            columns[0].add(String.valueOf(count));
            for (int i = 1; i <= columnsNumber; i++) {
                String value = rs.getString(i);
                value = value == null ? "null" : value;
                columns[i].add(value);
                lengths[i] = Math.max(lengths[i], value.length());
            }
            if (count > limit) break;
            count++;
        }

        lengths[0] = String.valueOf(count).length();

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < Math.min(count, limit); i++) {
            for (int j = 0; j <= columnsNumber; j++) {
                sb.append(String.format(" %-" + lengths[j] + "s ", columns[j].get(i)));
            }
            sb.append("\n");
        }

        System.out.print(sb.toString());
    }

    public void queryPrimaryTitlesByYear(int year) throws SQLException {
        // Get the connection
        this.getConnection();

        // Specify the query
        String query =  "SELECT titles.primary_title " +
                "FROM titles " +
                "WHERE titles.start_year = ?";

        PreparedStatement statement = this.connection.prepareStatement(query, ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_READ_ONLY);

        // Set the prepared statement parameters
        statement.setInt(1, year);

        // Execute the query
        try{
            ResultSet rs = statement.executeQuery();
            // Pretty print the result set
            printResultSet(rs, 100);
            rs.last();
            int resultCount = rs.getRow();
            rs.beforeFirst();
            System.out.println("Total results: " + resultCount);

            // Close the statement
            rs.close();
            statement.close();
        }catch(Exception ex) {
            System.out.println("Error: " + ex.getMessage());
        }

    }


    public void queryCharacterNamesByTitleContainingString(String keyString) throws SQLException {
        // Get the connection
        this.getConnection();

        // Specify the query
        String query =  "SELECT title_person_character.character_name " +
                "FROM title_person_character, titles " +
                "WHERE title_person_character.title_id = titles.title_id " +
                "AND titles.primary_title LIKE ?";

        PreparedStatement statement = this.connection.prepareStatement(query, ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_READ_ONLY);


        // Set the prepared statement parameters
        statement.setString(1, "%" + keyString + "%");

        // Execute the query
        try{

            ResultSet rs = statement.executeQuery();
            // Pretty print the result set
            printResultSet(rs, 100);
            rs.last();
            int resultCount = rs.getRow();
            rs.beforeFirst();
            System.out.println("Total results: " + resultCount);

            // Close the statement
            rs.close();
            statement.close();
        }catch(Exception ex) {
            System.out.println("Error: " + ex.getMessage());
        }

    }
    public void getPrimaryTitlesOfGenre(String genre) throws SQLException {
        // Get the connection
        this.getConnection();

        // Specify the query
        String query =  "SELECT titles.primary_title\n" +
                "FROM titles_genres, titles\n" +
                "WHERE titles_genres.title_id = titles.title_id\n" +
                "AND titles_genres.genre = ?";

        PreparedStatement statement = this.connection.prepareStatement(query, ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_READ_ONLY);


        // Set the prepared statement parameters
        statement.setString(1, genre);

        // Execute the query
        try{

            ResultSet rs = statement.executeQuery();
            // Pretty print the result set
            printResultSet(rs, 100);
            rs.last();
            int resultCount = rs.getRow();
            rs.beforeFirst();
            System.out.println("Total results: " + resultCount);

            // Close the statement
            rs.close();
            statement.close();
        }catch(Exception ex) {
            System.out.println("Error: " + ex.getMessage());
        }

    }

    public void getJobsAssociatedWithName(String name) throws SQLException {
        // Get the connection
        this.getConnection();

        // Specify the query
        String query =  "SELECT DISTINCT cast_info.job_category\n" +
                "FROM persons, cast_info\n" +
                "WHERE cast_info.person_id = persons.person_id\n" +
                "AND persons.full_name LIKE ?";

        PreparedStatement statement = this.connection.prepareStatement(query, ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_READ_ONLY);


        // Set the prepared statement parameters
        statement.setString(1, name);

        // Execute the query
        try{

            ResultSet rs = statement.executeQuery();
            // Pretty print the result set
            printResultSet(rs, 100);
            rs.last();
            int resultCount = rs.getRow();
            rs.beforeFirst();
            System.out.println("Total results: " + resultCount);

            // Close the statement
            rs.close();
            statement.close();
        }catch(Exception ex) {
            System.out.println("Error: " + ex.getMessage());
        }

    }

    public static void main(String[] args) throws SQLException {
        // Create the connection
        // Put your credentials (postgres user, password) here
        JDBCUtil util = new JDBCUtil("postgres", "1234", "imdb");

        // Run the query
        //util.queryPrimaryTitlesByYear(2022);
        //util.queryCharacterNamesByTitleContainingString("Star Wars");
        //util.getPrimaryTitlesOfGenre("Comedy");
        util.getJobsAssociatedWithName("Steven Spielberg");
    }
}
