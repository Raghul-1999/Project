using Microsoft.Extensions.Logging;
using Snowflake.Data.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SnowflakeCostAndUsageFA {
  public class Snowflake {
    //Method for Snowflake connection
    public static void getSnowflakeConnection(String query, String snowflakeConnectionString, ILogger log) {
      using(IDbConnection conn = new SnowflakeDbConnection()) {
        try {
          log.LogInformation("Connecting to Snowflake...");

          conn.ConnectionString = snowflakeConnectionString;
          if (query.ToUpper().Equals("TRUNCATE")) {
            log.LogInformation("Table Truncation Initated...");
            string truncQry = "Query;";
            truncate the table
            CreateTable(conn, truncQry);
            log.LogInformation("Table Truncation completed...");
          } else {
            log.LogInformation("Insert values...");
            CreateTable(conn, query);
            log.LogInformation("Insertion completed...");
          }

        } catch (Exception err) {
          log.LogInformation(err.ToString());

        } finally {
          conn.Close();
          log.LogInformation("Connection closed...");
        }
      }
    }

    public static void CreateTable(IDbConnection conn, String query) {
      conn.Open();
      var cmd = conn.CreateCommand();
      cmd.CommandText = query;
      cmd.ExecuteNonQuery();
    }
  }

}