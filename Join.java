import java.sql.*;
import java.util.*;

public class Join {
    public static void main(String[] args) {
        try {
            // Loading the PostgresSQL JDBC driver
            Class.forName("org.postgresql.Driver");

            // Checking if all the necessary arguments are provided or not
            if (args.length < 1) {
                System.err.println("Enter the database connection URL along with the username and password as a command line argument!");
                System.exit(1);
            }

            if (args.length < 2) {
                System.err.println("Enter two relations as command line arguments");
                System.exit(1);
            }
            if (args.length < 3) {
                System.err.println("Enter one more relation as a command line argument");
                System.exit(1);
            }

            // Get connection to the PostgresSQL by creating an object from DbConnection class.
            String url = args[0];
            Connection conn = DbConnection.getConnection(url);

            // Accessing Metadata to know about Schema, Primary Keys and Foreign Keys.
            DatabaseMetaData metadata = conn.getMetaData();

            // Gather the names of Relations/Tables.
            String Relation1 = args[1];
            String Relation2 = args[2];

            ResultSet resultSet1 = metadata.getTables(null, null, Relation1, null);
            if (!resultSet1 .next()) {
                System.out.println("Relation 1 does not exist in the database");
                System.exit(1);
            }
            ResultSet resultSet2 = metadata.getTables(null, null, Relation2, null);
            if (!resultSet2.next()) {
                System.out.println("Relation2 does not exist in the database");
                System.exit(1);
            }

            // Create an object for DatabaseHandler class
            DatabaseHandler dbHandler = new DatabaseHandler(conn);

            // Count the number of rows/tuples in the relations.
            int n1 = dbHandler.getRowCount(Relation1);
            int n2 = dbHandler.getRowCount(Relation2);

            // Use getColumns method of DatabaseHandler class to get columns for both Relations.
            ArrayList<String> columns1 = dbHandler.getColumns(Relation1);
            ArrayList<String> columns2 = dbHandler.getColumns(Relation2);

            // Get the common columns between the two Relations.
            ArrayList<String> commonColumns = new ArrayList<String>();
            for (String s : columns1) {
                if (columns2.contains(s)) {
                    commonColumns.add(s);
                }
            }

            // Get the Primary Keys for both Relations.
            ArrayList<String> PrimaryKeys1 = dbHandler.getPrimaryKeys(Relation1);
            ArrayList<String> PrimaryKeys2 = dbHandler.getPrimaryKeys(Relation2);

            // Get the Foreign Keys for both Relations.
            ArrayList<String> ForeignKeys1 = dbHandler.getForeignKeys("Relation1");
            ArrayList<String> ForeignKeys2 = dbHandler.getForeignKeys("Relation2");

            Collections.sort(columns1);
            Collections.sort(columns2);
            Collections.sort(commonColumns);
            Collections.sort(PrimaryKeys1);
            Collections.sort(PrimaryKeys2);
            Collections.sort(ForeignKeys1);
            Collections.sort(ForeignKeys2);

            // Initializing the estimated join size to 0.
            int estimatedValue = 0;

            // Condition when there are no common columns.
            if (commonColumns.isEmpty()) {
                estimatedValue = n1 * n2;
            }

            // Condition when the common columns is the key for Relation1.
            else if (commonColumns.equals(PrimaryKeys1)) {
                estimatedValue = n2;
            }

            // Condition when the common columns is the key for Relation2.
            else if (commonColumns.equals(PrimaryKeys2)) {
                estimatedValue = n1;
            }

            // Condition when the common columns is the foreign key for Relation2 referencing Relation1.
            else if (ForeignKeys2.equals(PrimaryKeys1) && commonColumns.equals(ForeignKeys2)) {
                estimatedValue = n2;
            }

            // Condition when the common columns is the foreign key for Relation1 referencing Relation2.
            else if (ForeignKeys1.equals(PrimaryKeys2) && commonColumns.equals(ForeignKeys1)) {
                estimatedValue = n1;
            }

            else {
                // Query for number of distinct values that appear in Relation1 for common attribute A
                String query5 = "select count (distinct " + commonColumns.get(0) + ") from " + Relation1;

                PreparedStatement preparedStatement5 = conn.prepareStatement(query5);
                ResultSet resultSet11 = preparedStatement5.executeQuery();
                int vas1 = 1;
                if (resultSet11.next()) {
                    vas1 = resultSet11.getInt(1);
                }

                // Query for number of distinct values that appear in Relation2 for common attribute A
                String query6 = "select count (distinct " + commonColumns.get(0) + ") from " + Relation2;

                PreparedStatement preparedStatement6 = conn.prepareStatement(query6);
                ResultSet resultSet12 = preparedStatement6.executeQuery();
                int vas2 = 1;
                if (resultSet12.next()) {
                    vas2 = resultSet12.getInt(1);
                }

                // The estimated join size is the minimum of the two values computed above when the common attributes is not primary key or foreign key for any of the two relations.
                estimatedValue = Math.min(n1 * n2 / vas1, n1 * n2 / vas2);
            }

            // Query for finding the Actual Join Size between two Relations.
            String query7 = "Select count(*) from " + Relation1 + " natural join " + Relation2;

            PreparedStatement preparedStatement7 = conn.prepareStatement(query7);
            ResultSet resultSet13 = preparedStatement7.executeQuery();
            int actualValue = 1;
            if (resultSet13.next()) {
                actualValue = resultSet13.getInt(1);
            }

            //Actual Join Size, Estimated Join Size and the Estimation Error
            System.out.println("Actual Join Size:" + actualValue);
            System.out.println("Estimated Join Size:" + estimatedValue);
            int error = estimatedValue - actualValue;
            System.out.println("Estimation Error:" + error);

            // Close all the resources and connections.
            resultSet1.close();
            resultSet2.close();
            resultSet13.close();
            preparedStatement7.close();

            if (conn != null) {
                conn.close();
            }

        } catch (SQLException | ClassNotFoundException exception) {
            exception.printStackTrace();
        }
    }
}
