package com.a2.assignment.music;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

import java.util.HashMap;
import java.util.Map;

public class ScanByAlbum {
    public static void main(String[] args) {
        String tableName = "music";
        String albumToSearch = "There's No Leaving Now";

        Region region = Region.US_EAST_1; // change if needed

        try (DynamoDbClient dynamoDb = DynamoDbClient.builder()
                .region(region)
                .build()) {

            Map<String, String> expressionNames = new HashMap<>();
            expressionNames.put("#al", "album");

            Map<String, AttributeValue> expressionValues = new HashMap<>();
            expressionValues.put(":albumValue", AttributeValue.builder().s(albumToSearch).build());

            ScanRequest request = ScanRequest.builder()
                    .tableName(tableName)
                    .filterExpression("#al = :albumValue")
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