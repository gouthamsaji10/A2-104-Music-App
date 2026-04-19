package com.a2.assignment.music;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import java.util.HashMap;
import java.util.Map;

public class QueryByTitle {
    public static void main(String[] args) {
        String tableName = "music";
        String titleToSearch = "1904";

        Region region = Region.US_EAST_1; // change if needed

        try (DynamoDbClient dynamoDb = DynamoDbClient.builder()
                .region(region)
                .build()) {

            Map<String, String> expressionNames = new HashMap<>();
            expressionNames.put("#t", "title");

            Map<String, AttributeValue> expressionValues = new HashMap<>();
            expressionValues.put(":titleValue", AttributeValue.builder().s(titleToSearch).build());

            QueryRequest request = QueryRequest.builder()
                    .tableName(tableName)
                    .keyConditionExpression("#t = :titleValue")
                    .expressionAttributeNames(expressionNames)
                    .expressionAttributeValues(expressionValues)
                    .build();

            QueryResponse response = dynamoDb.query(request);

            System.out.println("Songs found: " + response.count());
            response.items().forEach(item -> {
                System.out.println("Title: " + item.get("title").s());
                System.out.println("Artist: " + item.get("artist").s());
                System.out.println("Album: " + item.get("album").s());
                System.out.println("Year: " + item.get("year").s());
                System.out.println("Image URL: " + item.get("img_url").s());
                System.out.println("----------------------");
            });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}