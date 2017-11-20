// Copyright 2017 Amazon Web Services, Inc. or its affiliates. All rights reserved.

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

// The InfectionsDataUploader class reads infections data from a file and uploads each item to the infections table.
public class InfectionsDataUploader {

  public static final String INFECTIONS_TABLE_NAME = InfectionsTableCreator.INFECTIONS_TABLE_NAME;
  public static final Region REGION = Utils.getRegion();
  public static final String S3_BUCKET_NAME = Utils.LAB_S3_BUCKET_NAME;
  public static final String S3_BUCKET_REGION = Utils.LAB_S3_BUCKET_REGION;
  public static final String INFECTIONS_DATA_FILE_KEY = Utils.INFECTIONS_DATA_FILE_KEY;

  private static DynamoDB dynamoDB = null;
  private static AmazonDynamoDBClient dynamoDBClient = null;
  private static AmazonS3Client s3 = null;

  public static int numItemsAdded = 0;

  public static void main(String[] args) throws Exception {

    PropertiesCredentials pac = new PropertiesCredentials(new File("/.aws/credentials"));
    // Instantiate DynamoDB client and object
    dynamoDBClient = new AmazonDynamoDBClient(pac);
    dynamoDBClient.setRegion(REGION);
    dynamoDB = new DynamoDB(dynamoDBClient);

    // Instantiate S3 client
    ProfileCredentialsProvider pcp = new ProfileCredentialsProvider();

    System.err.println(pcp.getCredentials().getAWSAccessKeyId());
    System.err.println(pcp.getCredentials().getAWSSecretKey());
    BasicAWSCredentials bac = new BasicAWSCredentials(
        "AKIAJHEJUWR2YJWWDLDA",
        "SKH74ziyXzTz7d0g0Je/fxhgnD15NwRuu4dNliZi");

    s3 = new AmazonS3Client(pac);
    s3.setRegion(Region.getRegion(Regions.fromName(S3_BUCKET_REGION)));
    System.out.println("file from " + s3.getRegion());


    S3Object infectionsDataObject = null;
    BufferedReader br = null;
    String line = "";
    String splitter = ",";
    PutItemOutcome outcome = null;

    try {
      // Retrieve the infections data file from the S3 bucket
      infectionsDataObject = s3.getObject(S3_BUCKET_NAME, INFECTIONS_DATA_FILE_KEY);
      if (infectionsDataObject == null) {
        System.out.println("Unable to retrieve infections data file");
        return;
      }

      // Retrieve the Table object for the infections table
      Table table = dynamoDB.getTable(INFECTIONS_TABLE_NAME);

      br = new BufferedReader(new InputStreamReader(infectionsDataObject.getObjectContent()));
      // Skip the first line because it contains headings
      br.readLine();

      while ((line = br.readLine()) != null) {
        // Split line into values using comma as the separator
        String[] infectionsDataAttrValues = line.split(splitter);

        if (!infectionsDataAttrValues[0].equals("PatientId")) {

          // Add an item corresponding to the values in the line
          // CSV attributes: PatientId, City, Date

System.err.println(table);
System.err.println(                  infectionsDataAttrValues[0] + "\n" +
    infectionsDataAttrValues[1] + "\n" +
    infectionsDataAttrValues[2]);

          outcome =
              addItemToTable(
                  table,
                  infectionsDataAttrValues[0],
                  infectionsDataAttrValues[1],
                  infectionsDataAttrValues[2]);

          if (outcome != null) {
            numItemsAdded++;
            System.out.println("Added item:" + line);
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      numItemsAdded = 0;
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      System.out.println("\nNumber of items added: " + numItemsAdded);
    }
    System.out.println("Data upload complete");
  }

  /**
   * Add a record to the DynamoDB table
   *
   * @param table       Table object to update
   * @param patientId   Patient ID
   * @param city        City
   * @param date        Date
   * @return            Addition result
   */
  public static PutItemOutcome addItemToTable(
      Table table, String patientId, String city, String date) {
    // STUDENT TODO 2: Replace the solution with your own code
    return Solution.addItemToTable(table, patientId, city, date);
  }
}
