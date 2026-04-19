package com.a2.assignment.music;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

import java.util.HashMap;
import java.util.Map;

public class ScanByYear {
    public static void main(String[] args) {
        String tableName = "music";
        String yearToSearch = "2012";

        Region region = Region.US_EAST_1; // change if needed

        try (DynamoDbClient dynamoDb = DynamoDbClient.builder()
                .region(region)
                .build()) {

            Map<String, String> expressionNames = new HashMap<>();
            expressionNames.put("#y", "year");

            Map<String, AttributeValue> expressionValues = new HashMap<>();
            expressionValues.put(":yearValue", AttributeValue.builder().s(yearToSearch).build());

            ScanRequest request = ScanRequest.builder()
                    .tableName(tableName)
                    .filterExpression("#y = :yearValue")
                    .expressionAttributeNames(expressionNames)
                    .expressionAttributeValues(expressionValues)
                    .build();

            ScanResponse response = dynamoDb.scan(request);

            System.out.println("Songs found: " + response.count());
            response.items().forEach(item -> {
                System.out.println("Title: " + item.get("title").s());
                System.out.println("Artist: " + item.get("artist").s());
                System.out.println("Album: " + item.get("album").s());
                System.out.println("Year: " + item.get("year").s());
                System.out.println("----------------------");
            });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}