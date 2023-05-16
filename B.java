import java.sql.*;
import java.util.*;

public class B {
  public static void main(String[] args) {
    try {
      // Loading the PostgreSQL JDBC driver
      Class.forName("org.postgresql.Driver");

      // Establishing a connection to the database using the following credentials
      String url = "jdbc:postgresql://localhost:5433/Data";
      String user = "Username";
      String password = "Password";
      if(args.length < 1) {
    	  System.err.println("Enter two relations as arguments");
    	  System.exit(1);
      }
      if(args.length < 2) {
    	  System.err.println("Enter one more relation as argument");
    	  System.exit(1);
      }
      String Relation1 = args[0];
      String Relation2 = args[1];
      //Establishing connection
      Connection conn = DriverManager.getConnection(url, user, password);
      //Obtaining Meta Data for getting information about Schema, Primary Keys and Foreign Keys.
      DatabaseMetaData metadata = conn.getMetaData();
      
      ResultSet rs0 = metadata.getTables(null, null, Relation1, null);
      if(!rs0.next()) {
    	  System.out.println("The entered relation1 does not exist");
    	  System.exit(1);
      }
      ResultSet rs00 = metadata.getTables(null, null, Relation2, null);
      if(!rs00.next()) {
    	  System.out.println("The entered relation2 does not exist");
    	  System.exit(1);
      }
      

      //Obtaining the number of tuples in Relation1.
      String Query_No_Of_Tuples_Relation1 = "Select count(*) from " + Relation1;
      PreparedStatement ps1 = conn.prepareStatement(Query_No_Of_Tuples_Relation1);
      ResultSet rs1 = ps1.executeQuery();
      int n1 = 0;
      if (rs1.next()) {
          n1 = rs1.getInt(1);
      }
      
      
      //Obtaining the number of tuples in Relation2.
      String Query_No_Of_Tuples_Relation2 = "Select count(*) from " + Relation2;
      PreparedStatement ps2 = conn.prepareStatement(Query_No_Of_Tuples_Relation2);
      ResultSet rs2 = ps2.executeQuery();
      int n2 = 0;
      if (rs2.next()) {
          n2 = rs2.getInt(1);
      }
      
      
      
      //Obtaining the columns of Relation1.
      String q1 = "Select * from " + Relation1;
      PreparedStatement ps3 = conn.prepareStatement(q1);
      ResultSet rs3 = ps3.executeQuery();
      ResultSetMetaData rs3MetaData = rs3.getMetaData();
      
      int count1 = rs3MetaData.getColumnCount();
      ArrayList<String> columns1 = new ArrayList<String>();
      for(int i = 1; i<=count1; i++) {
         columns1.add(rs3MetaData.getColumnName(i));
      }
      
      
     
      //Obtaining the columns of Relation2.
      String q2 = "Select * from " + Relation2;
      PreparedStatement ps4 = conn.prepareStatement(q2);
      ResultSet rs4 = ps4.executeQuery();
      ResultSetMetaData rs4MetaData = rs4.getMetaData();
      
      int count2 = rs4MetaData.getColumnCount();
      ArrayList<String> columns2 = new ArrayList<String>();
      for(int i = 1; i<=count2; i++) {
         columns2.add(rs4MetaData.getColumnName(i));
      }
      
      
      //Common columns between the two Relations.
      ArrayList<String> commoncolumns = new ArrayList<String>();
      for (String s : columns1) {
          if (columns2.contains(s)) {
              commoncolumns.add(s);
          }
      }
      
      
      
      
      //Obtaining Primary Keys for Relation1.
      ResultSet rs5 = metadata.getPrimaryKeys(null, null, Relation1);
      ArrayList<String> PrimaryKeys1 = new ArrayList<>();
      while(rs5.next()) {
    	  PrimaryKeys1.add(rs5.getString("COLUMN_NAME"));
      }
      
      
      //Obtaining Primary Keys for Relation2.
      ResultSet rs6 = metadata.getPrimaryKeys(null, null, Relation2);
      ArrayList<String> PrimaryKeys2 = new ArrayList<>();
      while(rs6.next()) {
    	  PrimaryKeys2.add(rs6.getString("COLUMN_NAME"));
      }
      
      
      //Obtaining Foreign Keys for Relaion1.
      ResultSet rs7 = metadata.getImportedKeys(null, null, Relation1);
      ArrayList<String> ForeignKeys1 = new ArrayList<>();
      while(rs7.next()) {
    	  ForeignKeys1.add(rs7.getString("FKCOLUMN_NAME"));
      }
      
      
      //Obtaining Foreign Keys for Relation2.
      ResultSet rs8 = metadata.getImportedKeys(null, null, Relation2);
      ArrayList<String> ForeignKeys2 = new ArrayList<>();
      while(rs8.next()) {
    	  ForeignKeys2.add(rs8.getString("FKCOLUMN_NAME"));
      }
     
      
      
      Collections.sort(columns1);
      Collections.sort(columns2);
      Collections.sort(commoncolumns);
      Collections.sort(PrimaryKeys1);
      Collections.sort(PrimaryKeys2);
      Collections.sort(ForeignKeys1);
      Collections.sort(ForeignKeys2);
      
      //Initializing the estimated join size and updating it accordingly. 
      int est_value = 0; 
      //There are no common columns. 
      if(commoncolumns.isEmpty()) {
    	  est_value = n1 * n2;
      }
      //The common columns is the key for Relation1.
      else if(commoncolumns.equals(PrimaryKeys1)) {
    	  est_value = n2;
      }
      //The common columns is the key for Relation2.
      else if(commoncolumns.equals(PrimaryKeys2)) {
    	  est_value = n1;
      }
      //The common columns is the foreign key for Relation2 referencing Relation1.
      else if (ForeignKeys2.equals(PrimaryKeys1) && commoncolumns.equals(ForeignKeys2)) {
    	  est_value = n2;
      }
      //The common columns is the foreign key for Relation1 referencing Relation2.
      else if (ForeignKeys1.equals(PrimaryKeys2) && commoncolumns.equals(ForeignKeys1)) {
    	  est_value = n1;
      }
      else {   
      //Query for number of distinct values that appear in Relation1 for common attribute A
      String QueryforVAS1 = "select count (distinct " + commoncolumns.get(0) + ") from " + Relation1;
      PreparedStatement ps9 = conn.prepareStatement(QueryforVAS1);
      ResultSet rs9 = ps9.executeQuery();
      int vas1 = 1;
      if(rs9.next()) {
    	  vas1 = rs9.getInt(1);
      }
     //Query for number of distinct values that appear in Relation2 for common attribute A
      String QueryforVAS2 = "select count (distinct " + commoncolumns.get(0) + ") from " + Relation2;
      PreparedStatement ps10 = conn.prepareStatement(QueryforVAS2);
      ResultSet rs10 = ps10.executeQuery();
      int vas2 = 1;
      if(rs10.next()) {
    	  vas2 = rs10.getInt(1);
      }
      
      //The estimated join size is the minimum of the two values computed above when the common attributes is not primary key or foreign key for any of the two relations.
      est_value = Math.min(n1*n2/vas1, n1*n2/vas2);
      //Printing the Estimated Join size.
      
      }
      //Query for finding the Actual Join Size between two Relations.
      String Query_Tuples_In_Actual_Join = "Select count(*) from " + Relation1 + " natural join " + Relation2; 
      PreparedStatement ps11 = conn.prepareStatement(Query_Tuples_In_Actual_Join);
      ResultSet rs11 = ps11.executeQuery();
      int act_value = 1;
      if(rs11.next()) {
    	  act_value = rs11.getInt(1);
      }
      //Printing the Actual Join Size and the Estimation Error.  
      System.out.println("Actual Join Size:" + act_value);
      System.out.println("Estimated Join Size:"+ est_value);
      int err_value = est_value - act_value;
      System.out.println("Estimation Error:" + err_value);
      
      //Closing all the resources and connections.
      rs1.close();
      ps1.close();
      ps2.close();
      ps3.close();
      ps4.close();
      //ps10.close();
      ps11.close();
      rs2.close();
      rs3.close();
      rs4.close();
      rs5.close();
      rs6.close();
      rs7.close();
      rs8.close();
      //rs9.close();
      //rs10.close();
      rs11.close();
      conn.close();
      rs0.close();
      rs00.close();
    } catch (SQLException | ClassNotFoundException exception) {
      exception.printStackTrace();
    }
  }
}

