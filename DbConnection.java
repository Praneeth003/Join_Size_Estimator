import java.sql.*;
import java.util.*;
import java.net.*;

public class DbConnection {

    public static Connection getConnection(String url) throws SQLException {
        try {

            Class.forName("org.postgresql.Driver");

            //Exception Handling
            try {
                // Get the location of the database server. In our case, it is the localhost
                String url1 = url.substring(1, url.length() - 1);
                URI connectionURI = new URI(url1);

                int questionMarkIndex = url.indexOf('?');

                if(questionMarkIndex == -1){
                    System.out.println("Invalid URL");
                    System.exit(1);
                }

                // Creating objects for extractValue class
                String user = extractValue(url, "user");
                String password = extractValue(url, "password");

                // Validate extracted credentials
                if (user == null || password == null) {
                    throw new SQLException("Missing user or password in the connection URL!");
                }

                // Establish a connection
                return DriverManager.getConnection(url, user, password);

            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    // Implement a class to extract the values of a key in an url.
    private static String extractValue(String url, String key) {
        int startIndex = url.indexOf(key + "=");
        if (startIndex != -1) {
            startIndex += key.length() + 1; // Move to the start of the value
            int endIndex = url.indexOf('&', startIndex);
            if (endIndex != -1) {
                return url.substring(startIndex, endIndex);
            } else {
                return url.substring(startIndex);
            }
        }
        return null;
    }
}
