package com.a2.assignment.music;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;

import java.util.HashMap;
import java.util.Map;

public class QueryLoginUser {
    public static void main(String[] args) {
        String tableName = "login";
        String emailToSearch = "s41355980@student.rmit.edu.au";

        Region region = Region.US_EAST_1;

        try (DynamoDbClient dynamoDb = DynamoDbClient.builder()
                .region(region)
                .build()) {

            Map<String, AttributeValue> key = new HashMap<>();
            key.put("email", AttributeValue.builder().s(emailToSearch).build());

            GetItemRequest request = GetItemRequest.builder()
                    .tableName(tableName)
                    .key(key)
                    .build();

            GetItemResponse response = dynamoDb.getItem(request);

            if (response.hasItem()) {
                System.out.println("User found:");
                System.out.println("Email: " + response.item().get("email").s());
                System.out.println("Username: " + response.item().get("user_name").s());
                System.out.println("Password: " + response.item().get("password").s());
            } else {
                System.out.println("User not found.");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}