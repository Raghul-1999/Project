using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using System;
using System.Text.RegularExpressions;
using Snowflake.Data.Client;
using Microsoft.Rest;
using System.IO;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Azure.KeyVault;
using Microsoft.Azure.KeyVault.Models;
using Microsoft.Rest.Azure.Authentication;
using Microsoft.Azure.OperationalInsights;
using Newtonsoft.Json;

namespace PipelineMonitorApp {
  public static class PipelineMonitorApplication {
    public static string StringClientID = "";

    public static string StringClientSecret = "";

    public static string StringWorksapceID = "";

    public static string snowflakeConnectionString = "";

    public static string DatabaseName = "";


    public static string Monday = "";

    [FunctionName("PipelineMonitorApplicationFunction")]
    public static async Task < string > RunOrchestrator(ILogger log,
      [OrchestrationTrigger] IDurableOrchestrationContext context) {
      string name = "Suceess";
      //SayHello(log);

      OrchestratorRequest orData = context.GetInput < OrchestratorRequest > ();
      //orData.InstanceId = context.InstanceId;
      //PayloadRequest payloadRequest = orData.UserParam;

      var PipelineStatus = await context.CallActivityAsync < string > ("PipelineMonitorApplicationFunction2", JsonConvert.SerializeObject(orData));

      if (PipelineStatus.StartsWith("Exception")) {
        throw new Exception("Exception occurred in PipelineMonitorApplicationFunction2, Exception Detail : " + PipelineStatus);
      }

      return name;
    }

    [FunctionName("PipelineMonitorApplicationFunction2")]
    public static string SayHello([ActivityTrigger] string payloadRequestString, ILogger log) {
      OrchestratorRequest ordata = null;
      try {
        ordata = JsonConvert.DeserializeObject < OrchestratorRequest > (payloadRequestString);

        //Log Analytics Connection
        var workspaceId = StringWorksapceID; //workspaceID
        var clientId = StringClientID; //clientID
        var clientSecret = StringClientSecret; //clientSecret

        var domain = "*******************"; //Domain
        var authEndpoint = "******************"; //authEndpoint
        var tokenAudience = "********************"; //tokenAudience

        var adSettings = new ActiveDirectoryServiceSettings {
          AuthenticationEndpoint = new Uri(authEndpoint),
            TokenAudience = new Uri(tokenAudience),
            ValidateAuthority = true
        };

        var creds = ApplicationTokenProvider.LoginSilentAsync(domain, clientId, clientSecret, adSettings).GetAwaiter().GetResult();

        var client = new OperationalInsightsDataClient(creds);
        client.WorkspaceId = workspaceId;

        string Daily24hMonday72h;

        DateTime currentDateTime = DateTime.Now;
        int week = (int) currentDateTime.DayOfWeek;

        Console.WriteLine(week);

        if (week == 1) {
          Daily24hMonday72h = "72h";
          Console.WriteLine(Daily24hMonday72h);
        } else {
          Daily24hMonday72h = "24h";
          Console.WriteLine(Daily24hMonday72h);
        }

        //In ADF check last 24 hours of ADFActivityRUN,ADFPipelineRun,ADFTriggerRun are InProgress or Queued 
        string[] query = new string[] {
          "ADFActivityRun | where TimeGenerated > ago(" + Daily24hMonday72h + ") | where Status !in ('InProgress', 'Queued')",
            "ADFPipelineRun | where TimeGenerated  > ago(" + Daily24hMonday72h + ")| where Status !in ('InProgress', 'Queued')",
            "ADFTriggerRun | where TimeGenerated  > ago(" + Daily24hMonday72h + ") | where Status !in ('InProgress', 'Queued')"
        };
        SnowflakeConnection.getSnowflakeConnection("Truncate");
        foreach(var qry in query) {
          var results = client.Query(qry);
          dynamic jsonObj = JsonConvert.SerializeObject(results.Results);

          //ADFActivityRUn insterted in the Snowflake table
          if (qry.Contains("ADFActivityRun")) {

            var myDeserializedClass = JsonConvert.DeserializeObject < List < ADFActitivtyRun >> (jsonObj);
            log.LogInformation("\nADF_ACTIVITY_RUN Table insert scripts are initiated.....");
            inserADFActitivtyRunData(myDeserializedClass, log);
            log.LogInformation("ADF_ACTIVITY_RUN Table insert scripts are completed.....");
          }
          //ADFPipelineRun insterted in the Snowflake table
          else if (qry.Contains("ADFPipelineRun")) {
            var myDeserializedClass = JsonConvert.DeserializeObject < List < ADFPipelineRun >> (jsonObj);
            log.LogInformation("\nADF_PIPELINE_RUN Table insert scripts are initiated.....");
            inserADFPipelineRunData(myDeserializedClass, log);
            log.LogInformation("ADF_PIPELINE_RUN Table insert scripts are completed.....");
          }
          //ADF_Trigger_Run insterted in the Snowflake table
          else {
            var myDeserializedClass = JsonConvert.DeserializeObject < List < ADFTriggerRun >> (jsonObj);
            log.LogInformation("\nADF_TRIGGER_RUN Table insert scripts are initiated.....");
            insertTriggerRunData(myDeserializedClass, log);
            log.LogInformation("ADF_TRIGGER_RUN Table insert scripts are completed.....");
          }
        }

        log.LogInformation("\nTables are Created sucessfully...");
        return "success";
      } catch (Exception ex) {
        string error = ex.ToString();
        string status = "Failed";
        return status;
      }

    }

    [FunctionName("PipelineMonitorApplicationFunction_HttpStart")]
    public static async Task < HttpResponseMessage > HttpStart(
      [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestMessage req,
      [DurableClient] IDurableOrchestrationClient starter,
      ILogger log)

    {
      string str_payload = await req.Content.ReadAsStringAsync();
      PayloadRequest payloadRequest = JsonConvert.DeserializeObject < PayloadRequest > (str_payload);

      string WorkspaceID = GetSecretValue(payloadRequest.P_KEYVAULT_URL, payloadRequest.P_Keyvault_WorkspaceID, log).Result;
      if (WorkspaceID.ToUpper().Contains("ERROR:")) {
        throw new Exception("Exception occurred while retrieving " + payloadRequest.P_Keyvault_WorkspaceID + " secret vaule from keyvault");
      }

      StringWorksapceID = WorkspaceID;

      string clientID = GetSecretValue(payloadRequest.P_KEYVAULT_URL, payloadRequest.P_Keyvault_ClientID, log).Result;
      if (clientID.ToUpper().Contains("ERROR:")) {
        throw new Exception("Exception occurred while retrieving " + payloadRequest.P_Keyvault_ClientID + " secret vaule from keyvault");
      }

      StringClientID = clientID;

      string clientSecret = GetSecretValue(payloadRequest.P_KEYVAULT_URL, payloadRequest.P_Keyvault_ClientSecret, log).Result;
      if (clientSecret.ToUpper().Contains("ERROR:")) {
        throw new Exception("Exception occurred while retrieving " + payloadRequest.P_Keyvault_ClientSecret + " secret vaule from keyvault");
      }

      StringClientSecret = clientSecret;

      string snowflakePassword = GetSecretValue(payloadRequest.P_KEYVAULT_URL, payloadRequest.P_KEYVAULT_DB_PASSWORD_SECRET, log).Result;
      if (snowflakePassword.ToUpper().Contains("ERROR:")) {
        throw new Exception("Exception occurred while retrieving " + payloadRequest.P_KEYVAULT_DB_PASSWORD_SECRET + " secret vaule from keyvault");
      }
      string snowflakeAccountHost = GetSecretValue(payloadRequest.P_KEYVAULT_URL, payloadRequest.P_KEYVAULT_DB_HOSTNAME_SECRET, log).Result;
      if (snowflakeAccountHost.ToUpper().Contains("ERROR:")) {
        throw new Exception("Exception occurred while retrieving " + payloadRequest.P_KEYVAULT_DB_HOSTNAME_SECRET + " secret vaule from keyvault");
      }

      string[] snowflakeAccountHostNameArray = snowflakeAccountHost.Split(';');
      snowflakeAccountHost = snowflakeAccountHostNameArray[1].Replace("Server=", "");
      string[] snowflakeAccountNameArray = snowflakeAccountHost.Split('.');
      String snowflakeAccount = snowflakeAccountNameArray[0];

      string connectionString = "ACCOUNT=" + snowflakeAccount + ";HOST=" + snowflakeAccountHost + ";user=" + payloadRequest.P_USER_NAME + ";password=" + snowflakePassword + ";AUTHENTICATOR=snowflake;db=" + payloadRequest.P_DATABASE_NAME + ";schema=TEMP;WAREHOUSE={0};ROLE=" + payloadRequest.P_ROLE_NAME + ";";

      DatabaseName = payloadRequest.P_DATABASE_NAME;

      snowflakeConnectionString = connectionString;
      snowflakeConnectionString = string.Format(snowflakeConnectionString, payloadRequest.P_WAREHOUSE_NAME);

      EmailTo = (payloadRequest.P_To_Recepient);

      EmailCc = (payloadRequest.P_Cc_Recepient);

      OrchestratorRequest orData = new OrchestratorRequest {
        UserParam = payloadRequest,
          RequestTimestamp = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, TimeZoneInfo.FindSystemTimeZoneById("AUS Eastern Standard Time")),
          ConnectionString = snowflakeConnectionString,
          P_To_Recepient = (payloadRequest.P_To_Recepient),
          P_Cc_Recepient = (payloadRequest.P_Cc_Recepient)

      };

      // Function input comes from the request content.
      string instanceId = await starter.StartNewAsync("PipelineMonitorApplicationFunction", null);

      log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

      return starter.CreateCheckStatusResponse(req, instanceId);
    }
    //Insert ADFActitivtyRunData into the Snowflake table
    public static void inserADFActitivtyRunData(List < ADFActitivtyRun > myList, ILogger log) {
      try {

        int lstCnt = myList.Count;
        int partition = lstCnt / 10000;
        for (int cnt = 0; cnt <= partition; cnt++) {
          string qryValues = "";
          var partionObj = ADFActitivtyRun.GetPage(myList, cnt, 10000);
          partionObj.ForEach(item => {
            qryValues += String.Format(", ('{0}','{1}',)", item. < COLUMN01 > .Replace("'", "''"), item.S < COLUMN02 > .Replace("'", "''"));
          });
          string activityRunQry = "INSERT <COLUMN NAMES> FROM " + qryValues; //ADD RESCPECTIVE COLUMN NAMES

          SnowflakeConnection.getSnowflakeConnection(activityRunQry.Replace("VALUES,", "VALUES"));
        }
      } catch (Exception e) {
        log.LogError(e.ToString());
      }
    }
    //Insert ADFPipelineRunData into the Snowflake table
    public static void inserADFPipelineRunData(List < ADFPipelineRun > myList, ILogger log) {
      try {
        int lstCnt = myList.Count;
        int partition = lstCnt / 10000;
        for (int cnt = 0; cnt <= partition; cnt++) {
          string qryValues = "";
          var partionObj = ADFPipelineRun.GetPage(myList, cnt, 10000);
          partionObj.ForEach(item => {
            qryValues += String.Format(", ('{0}','{1}',)", item. < COLUMN01 > .Replace("'", "''"), item.S < COLUMN02 > .Replace("'", "''"));
          });
          string activityRunQry = "INSERT <COLUMN NAMES> FROM " + qryValues; //ADD RESCPECTIVE COLUMN NAMES

          SnowflakeConnection.getSnowflakeConnection(activityRunQry.Replace("VALUES,", "VALUES"));
        }
      } catch (Exception e) {
        log.LogError(e.ToString());
      }
    }
    //Insert TriggerRunData into the Snowflake table
    public static void insertTriggerRunData(List < ADFTriggerRun > myList, ILogger log) {
      try {

        int lstCnt = myList.Count;
        int partition = lstCnt / 10000;
        for (int cnt = 0; cnt <= partition; cnt++) {
          string qryValues = "";
          var partionObj = ADFTriggerRun.GetPage(myList, cnt, 10000);
          partionObj.ForEach(item => {
            qryValues += String.Format(", ('{0}','{1}',)", item. < COLUMN01 > .Replace("'", "''"), item.S < COLUMN02 > .Replace("'", "''"));
          });
          string activityRunQry = "INSERT <COLUMN NAMES> FROM " + qryValues; //ADD RESCPECTIVE COLUMN NAMES

          SnowflakeConnection.getSnowflakeConnection(activityRunQry.Replace("VALUES,", "VALUES"));
        }
      } catch (Exception e) {
        log.LogError(e.ToString());
      }
    }
    private static async Task < string > GetSecretValue(String keyVaultName, String secretName, ILogger log) {
      try {
        SecretBundle secretBundle;
        String keyValueDNS = "https://" + keyVaultName + ".vault.azure.net/secrets/" + secretName;
        var azureServiceTokenProvider = new AzureServiceTokenProvider();
        var keyVaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(azureServiceTokenProvider.KeyVaultTokenCallback));
        secretBundle = await keyVaultClient.GetSecretAsync(keyValueDNS).ConfigureAwait(false);
        String secret = secretBundle.Value;
        return secret;
      } catch (Exception ex) {
        var methodName = System.Reflection.MethodBase.GetCurrentMethod().Name;
        log.LogInformation("Exception occurred in " + methodName + " : " + ex.Message.ToString());
        return "Exception : " + ex.Message.ToString();
      }
    }

  }
  public class PayloadRequest {
    public string P_WAREHOUSE_NAME {
      get;
      set;
    }
    public string P_DATABASE_NAME {
      get;
      set;
    }
    public string P_ROLE_NAME {
      get;
      set;
    }
    public string P_USER_NAME {
      get;
      set;
    }
    public string P_KEYVAULT_DB_HOSTNAME_SECRET {
      get;
      set;
    }
    public string P_KEYVAULT_DB_PASSWORD_SECRET {
      get;
      set;
    }
    public string P_KEYVAULT_URL {
      get;
      set;
    }
    public string P_Keyvault_ClientID {
      get;
      set;
    }
    public string P_Keyvault_ClientSecret {
      get;
      set;
    }
    public string P_Keyvault_WorkspaceID {
      get;
      set;
    }
    public string P_ADF_PIPELINE_ID {
      get;
      set;
    }
    public string P_To_Recepient {
      get;
      set;
    }
    public string P_Cc_Recepient {
      get;
      set;
    }

  }

  public class OrchestratorRequest {
    public PayloadRequest UserParam {
      get;
      set;
    }
    public string InstanceId {
      get;
      set;
    }
    public DateTime RequestTimestamp {
      get;
      set;
    }
    public string ConnectionString {
      get;
      set;
    }
    public string P_To_Recepient {
      get;
      set;
    }
    public string P_Cc_Recepient {
      get;
      set;
    }
  }

}