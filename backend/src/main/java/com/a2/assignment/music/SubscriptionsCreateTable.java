package com.a2.assignment.music;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

public class SubscriptionsCreateTable {

    private static final String TABLE_NAME = "subscriptions";

    public static void main(String[] args) {

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(new ProfileCredentialsProvider("default"))
                .build();

        DynamoDB dynamoDB = new DynamoDB(client);

        try {
            System.out.println("Creating table: " + TABLE_NAME);

            CreateTableRequest request = new CreateTableRequest()
                    .withTableName(TABLE_NAME)

                    /*
                     Main table key design:

                     Partition key = email
                     Sort key = song_id

                     Reason:
                     One user can subscribe to many songs.
                     So all subscriptions for one user can be retrieved using email.
                     song_id makes each subscribed song unique for that user.
                     */
                    .withKeySchema(
                            new KeySchemaElement("email", KeyType.HASH),
                            new KeySchemaElement("song_id", KeyType.RANGE)
                    )

                    .withAttributeDefinitions(
                            new AttributeDefinition("email", ScalarAttributeType.S),
                            new AttributeDefinition("song_id", ScalarAttributeType.S)
                    )

                    .withBillingMode(BillingMode.PAY_PER_REQUEST);

            Table table = dynamoDB.createTable(request);

            System.out.println("Waiting for table to become active...");
            table.waitForActive();

            System.out.println("Success. Table created and active: " + table.getDescription().getTableName());

        } catch (ResourceInUseException e) {
            System.out.println("Table already exists: " + TABLE_NAME);
            System.out.println("If the key schema is wrong, delete the existing table first and run this program again.");

        } catch (Exception e) {
            System.err.println("Unable to create table: " + TABLE_NAME);
            System.err.println(e.getMessage());
        }
    }
}