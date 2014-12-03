//Compile
//javac -classpath sqljdbc4.jar:json.jar:mongo-java-driver-2.12.4.jar:. MSSql_Table_To_MongoDB_Collection.java
//Run
//java -classpath sqljdbc4.jar:json.jar:mongo-java-driver-2.12.4.jar:. MSSql_Table_To_MongoDB_Collection mssql_table_name number_of_rows_at_a_time


import com.mongodb.BasicDBObject;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONException;

import java.sql.*;
import java.util.*;


public class MSSql_Table_To_MongoDB_Collection
{
  public static void main(String[] args)
  {

    Connection connection = null;

    MongoClient mongoClient = null;
    DB db = null;
    DBCollection coll = null;
    try
    {
      mongoClient = new MongoClient( "localhost" , 27017 );
      db=mongoClient.getDB( "teamwork" );
      coll=db.getCollection(args[0]);

      // the sql server driver string
      Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

      // the sql server url
      String url = "jdbc:sqlserver://server_host:1433;DatabaseName=database_name";

      // get the sql server database connection
      connection = DriverManager.getConnection(url,"username", "password");


      System.out.println("Connected.");


      String SQL2 = "SELECT COUNT(*) AS total FROM "+args[0];
      Statement stmt2 = connection.createStatement();
      ResultSet rs2 = stmt2.executeQuery(SQL2);
      int total_rows=0;

      if(rs2.next())
      {
        total_rows=rs2.getInt(1);
        System.out.println("Importing "+rs2.getInt(1)+" rows...");

      }

      //how many rows to import at a time
      int limit=Integer.parseInt(args[1]);
      for(int k=0;k<total_rows/limit;k++)
      {
        long startTime = System.currentTimeMillis();

        int start=k*limit;
        int end=start+limit;
        System.out.println("start: "+start);

        // Create and execute an SQL statement that returns some data.
        String SQL = "SELECT * FROM ( SELECT *, ROW_NUMBER() OVER (ORDER BY id) as row FROM "+args[0]+" ) a WHERE row > "+start+" and row <= "+end;
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(SQL);

        JSONArray json = new JSONArray();
        ResultSetMetaData rsmd = rs.getMetaData();

        // Iterate through the data in the result set and display it.
        while (rs.next())
        {
          int numColumns = rsmd.getColumnCount();

          JSONObject obj = new JSONObject();



	  BasicDBObject doc = new BasicDBObject();

          for (int i=1; i<numColumns+1; i++) 
          {
            String column_name = rsmd.getColumnName(i);
            String field_name=column_name;

            if(field_name.equals("id")) field_name="_id";


            doc.append(field_name, rs.getObject(column_name));
         }


         coll.update(new BasicDBObject("_id", doc.get("_id")),
          new BasicDBObject("$set", doc), true, false);



       }

       long endTime   = System.currentTimeMillis();
       long totalTime = endTime - startTime;

       
       System.out.println((double)totalTime/1000.0+"s");

       }
       
    }
    catch (ClassNotFoundException e)
    {
      e.printStackTrace();
      System.exit(1);
    }
    catch (SQLException e)
    {
      e.printStackTrace();
      System.exit(2);
    }
    catch (Exception e)
    {
      e.printStackTrace();
      System.exit(2);
    }
  }


}

