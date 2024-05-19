import java.sql.*;
import java.util.ArrayList;

public class DatabaseHandler {
    private Connection conn;

    // Constructor to initialize connection
    public DatabaseHandler(Connection conn) {
        this.conn = conn;
    }

    // Method to get the Columns of a Relation.
    public ArrayList<String> getColumns(String Relation) throws SQLException {
        String query = "SELECT * FROM " + Relation;
        PreparedStatement preparedStatement = conn.prepareStatement(query);
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();

        int columnCount = metaData.getColumnCount();
        ArrayList<String> columns = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            columns.add(metaData.getColumnName(i));
        }
        return columns;
    }

    // Method to get the Primary Keys of a Relation.
    public ArrayList<String> getPrimaryKeys(String Relation) throws SQLException {
        DatabaseMetaData metadata = conn.getMetaData();
        ResultSet resultSet = metadata.getPrimaryKeys(null, null, Relation);

        ArrayList<String> primaryKeys = new ArrayList<>();
        while (resultSet.next()) {
            primaryKeys.add(resultSet.getString("COLUMN_NAME"));
        }
        return primaryKeys;
    }

    // Method to get the Foreign Keys of a Relation.
    public ArrayList<String> getForeignKeys(String Relation) throws SQLException {
        DatabaseMetaData metadata = conn.getMetaData();
        ResultSet resultSet = metadata.getImportedKeys(null, null, Relation);

        ArrayList<String> foreignKeys = new ArrayList<>();
        while (resultSet.next()) {
            foreignKeys.add(resultSet.getString("FKCOLUMN_NAME"));
        }
        return foreignKeys;
    }

    // Method to get the number of tuples in a Relation.
    public int getRowCount(String Relation) throws SQLException {
        String query = "SELECT COUNT(*) FROM " + Relation;
        PreparedStatement preparedStatement = conn.prepareStatement(query);
        ResultSet resultSet = preparedStatement.executeQuery();
        int rowCount = 0;
        if (resultSet.next()) {
            rowCount = resultSet.getInt(1);
        }
        return rowCount;
    }
}
