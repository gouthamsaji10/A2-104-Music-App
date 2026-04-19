package com.a2.assignment.music;

import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class LoadMusicData {

    public static void main(String[] args) {
        String tableName = "music";
        String jsonFilePath = "2026a2_songs.json";

        Region region = Region.US_EAST_1;

        try (DynamoDbClient dynamoDb = DynamoDbClient.builder()
                .region(region)
                .build()) {

            ObjectMapper mapper = new ObjectMapper();
            SongCollection songCollection = mapper.readValue(new File(jsonFilePath), SongCollection.class);

            int count = 0;

            for (Song song : songCollection.getSongs()) {
                Map<String, AttributeValue> item = new HashMap<>();
                item.put("title", AttributeValue.builder().s(song.getTitle()).build());
                item.put("artist", AttributeValue.builder().s(song.getArtist()).build());
                item.put("year", AttributeValue.builder().s(song.getYear()).build());
                item.put("album", AttributeValue.builder().s(song.getAlbum()).build());
                item.put("img_url", AttributeValue.builder().s(song.getImg_url()).build());

                PutItemRequest request = PutItemRequest.builder()
                        .tableName(tableName)
                        .item(item)
                        .build();

                dynamoDb.putItem(request);
                count++;
                System.out.println("Inserted: " + song.getTitle() + " - " + song.getArtist());

            }

            System.out.println("Total songs inserted: " + count);

        } catch (Exception e) {
            System.err.println("Error loading songs: " + e.getMessage());
            e.printStackTrace();
        }
    }
}