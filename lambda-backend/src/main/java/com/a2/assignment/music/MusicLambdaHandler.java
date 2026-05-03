package com.a2.assignment.music;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Base64;

public class MusicLambdaHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private static final String MUSIC_TABLE = "music";
    private static final String LOGIN_TABLE = "login";
    private static final String SUBSCRIPTIONS_TABLE = "subscriptions";

    private static final Gson gson = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {}.getType();

    private static final AmazonDynamoDB dynamoDb = AmazonDynamoDBClientBuilder.standard()
            .withRegion(Regions.US_EAST_1)
            .build();

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request, Context context) {
        try {
            String method = request.getHttpMethod();
            String path = normalizePath(request.getPath());

            if ("OPTIONS".equalsIgnoreCase(method)) {
                return response(200, Map.of("message", "CORS preflight OK"));
            }

            if ("GET".equalsIgnoreCase(method) && "/health".equals(path)) {
                return response(200, Map.of("message", "Java Lambda backend is working with DynamoDB"));
            }

            // Register and login
            if ("POST".equalsIgnoreCase(method) && "/register".equals(path)) {
                return registerUser(request);
            }

            if ("POST".equalsIgnoreCase(method) && "/login".equals(path)) {
                return loginUser(request);
            }

            // User CRUD on login table
            if ("GET".equalsIgnoreCase(method) && "/users".equals(path)) {
                return scanTable(LOGIN_TABLE);
            }

            if (path.startsWith("/users/")) {
                String email = getPathPart(path, 2);

                if ("GET".equalsIgnoreCase(method)) {
                    return getItem(LOGIN_TABLE, key("email", email));
                }

                if ("PUT".equalsIgnoreCase(method)) {
                    Map<String, Object> body = getBodyMap(request);
                    return updateItem(LOGIN_TABLE, key("email", email), body, Set.of("email"));
                }

                if ("DELETE".equalsIgnoreCase(method)) {
                    return deleteItem(LOGIN_TABLE, key("email", email));
                }
            }

            // Music CRUD
            if ("GET".equalsIgnoreCase(method) && "/music".equals(path)) {
                Map<String, String> queryParams = request.getQueryStringParameters();

                if (queryParams != null && queryParams.containsKey("artist")) {
                    return queryMusicByArtist(queryParams.get("artist"));
                }

                return scanTable(MUSIC_TABLE);
            }

            if ("POST".equalsIgnoreCase(method) && "/music".equals(path)) {
                return createMusic(request);
            }

            if (path.startsWith("/music/")) {
                String artist = getPathPart(path, 2);
                String songId = getPathPart(path, 3);

                if (artist == null || songId == null) {
                    return response(400, Map.of("error", "Both artist and song_id are required in the path"));
                }

                Map<String, AttributeValue> musicKey = new HashMap<>();
                musicKey.put("artist", new AttributeValue().withS(artist));
                musicKey.put("song_id", new AttributeValue().withS(songId));

                if ("GET".equalsIgnoreCase(method)) {
                    return getItem(MUSIC_TABLE, musicKey);
                }

                if ("PUT".equalsIgnoreCase(method)) {
                    Map<String, Object> body = getBodyMap(request);
                    return updateItem(MUSIC_TABLE, musicKey, body, Set.of("artist", "song_id"));
                }

                if ("DELETE".equalsIgnoreCase(method)) {
                    return deleteItem(MUSIC_TABLE, musicKey);
                }
            }

            // Subscription CRUD
            if ("GET".equalsIgnoreCase(method) && path.startsWith("/subscriptions/")) {
                String email = getPathPart(path, 2);
                return querySubscriptionsByEmail(email);
            }

            if ("POST".equalsIgnoreCase(method) && "/subscriptions".equals(path)) {
                return createSubscription(request);
            }

            if ("DELETE".equalsIgnoreCase(method) && path.startsWith("/subscriptions/")) {
                String email = getPathPart(path, 2);
                String songId = getPathPart(path, 3);

                if (email == null || songId == null) {
                    return response(400, Map.of("error", "Both email and song_id are required in the path"));
                }

                Map<String, AttributeValue> subscriptionKey = new HashMap<>();
                subscriptionKey.put("email", new AttributeValue().withS(email));
                subscriptionKey.put("song_id", new AttributeValue().withS(songId));

                return deleteItem(SUBSCRIPTIONS_TABLE, subscriptionKey);
            }

            return response(404, Map.of(
                    "error", "Route not found",
                    "method", method,
                    "path", path
            ));

        } catch (Exception e) {
            return response(500, Map.of(
                    "error", "Internal server error",
                    "details", e.getMessage()
            ));
        }
    }

    private APIGatewayProxyResponseEvent registerUser(APIGatewayProxyRequestEvent request) {
        Map<String, Object> body = getBodyMap(request);

        if (!body.containsKey("email")) {
            return response(400, Map.of("error", "email is required"));
        }

        Map<String, AttributeValue> item = jsonToItem(body);

        try {
            PutItemRequest putItemRequest = new PutItemRequest()
                    .withTableName(LOGIN_TABLE)
                    .withItem(item)
                    .withConditionExpression("attribute_not_exists(email)");

            dynamoDb.putItem(putItemRequest);

            return response(201, Map.of("message", "User registered successfully"));

        } catch (ConditionalCheckFailedException e) {
            return response(409, Map.of("error", "User already exists"));
        }
    }

    private APIGatewayProxyResponseEvent loginUser(APIGatewayProxyRequestEvent request) {
        Map<String, Object> body = getBodyMap(request);

        String email = stringValue(body.get("email"));
        String password = stringValue(body.get("password"));

        if (email == null) {
            return response(400, Map.of("error", "email is required"));
        }

        GetItemResult result = dynamoDb.getItem(new GetItemRequest()
                .withTableName(LOGIN_TABLE)
                .withKey(key("email", email)));

        if (result.getItem() == null || result.getItem().isEmpty()) {
            return response(401, Map.of("error", "Invalid email or password"));
        }

        AttributeValue storedPasswordValue = result.getItem().get("password");

        if (storedPasswordValue != null && password != null) {
            String storedPassword = storedPasswordValue.getS();

            if (!password.equals(storedPassword)) {
                return response(401, Map.of("error", "Invalid email or password"));
            }
        }

        return response(200, Map.of(
                "message", "Login successful",
                "email", email
        ));
    }

    private APIGatewayProxyResponseEvent createMusic(APIGatewayProxyRequestEvent request) {
        Map<String, Object> body = getBodyMap(request);

        if (!body.containsKey("artist") || !body.containsKey("song_id")) {
            return response(400, Map.of("error", "artist and song_id are required"));
        }

        dynamoDb.putItem(new PutItemRequest()
                .withTableName(MUSIC_TABLE)
                .withItem(jsonToItem(body)));

        return response(201, Map.of("message", "Music item created successfully"));
    }

    private APIGatewayProxyResponseEvent createSubscription(APIGatewayProxyRequestEvent request) {
        Map<String, Object> body = getBodyMap(request);

        if (!body.containsKey("email") || !body.containsKey("song_id")) {
            return response(400, Map.of("error", "email and song_id are required"));
        }

        dynamoDb.putItem(new PutItemRequest()
                .withTableName(SUBSCRIPTIONS_TABLE)
                .withItem(jsonToItem(body)));

        return response(201, Map.of("message", "Subscription created successfully"));
    }

    private APIGatewayProxyResponseEvent queryMusicByArtist(String artist) {
        Map<String, String> names = new HashMap<>();
        names.put("#artist", "artist");

        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":artist", new AttributeValue().withS(artist));

        QueryRequest queryRequest = new QueryRequest()
                .withTableName(MUSIC_TABLE)
                .withKeyConditionExpression("#artist = :artist")
                .withExpressionAttributeNames(names)
                .withExpressionAttributeValues(values);

        QueryResult result = dynamoDb.query(queryRequest);

        return response(200, itemsToList(result.getItems()));
    }

    private APIGatewayProxyResponseEvent querySubscriptionsByEmail(String email) {
        Map<String, String> names = new HashMap<>();
        names.put("#email", "email");

        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":email", new AttributeValue().withS(email));

        QueryRequest queryRequest = new QueryRequest()
                .withTableName(SUBSCRIPTIONS_TABLE)
                .withKeyConditionExpression("#email = :email")
                .withExpressionAttributeNames(names)
                .withExpressionAttributeValues(values);

        QueryResult result = dynamoDb.query(queryRequest);

        return response(200, itemsToList(result.getItems()));
    }

    private APIGatewayProxyResponseEvent scanTable(String tableName) {
        ScanResult result = dynamoDb.scan(new ScanRequest().withTableName(tableName));
        return response(200, itemsToList(result.getItems()));
    }

    private APIGatewayProxyResponseEvent getItem(String tableName, Map<String, AttributeValue> key) {
        GetItemResult result = dynamoDb.getItem(new GetItemRequest()
                .withTableName(tableName)
                .withKey(key));

        if (result.getItem() == null || result.getItem().isEmpty()) {
            return response(404, Map.of("error", "Item not found"));
        }

        return response(200, itemToMap(result.getItem()));
    }

    private APIGatewayProxyResponseEvent updateItem(
            String tableName,
            Map<String, AttributeValue> key,
            Map<String, Object> body,
            Set<String> keyAttributes
    ) {
        Map<String, String> names = new HashMap<>();
        Map<String, AttributeValue> values = new HashMap<>();
        List<String> updateParts = new ArrayList<>();

        int index = 0;

        for (Map.Entry<String, Object> entry : body.entrySet()) {
            String attributeName = entry.getKey();

            if (keyAttributes.contains(attributeName)) {
                continue;
            }

            String namePlaceholder = "#field" + index;
            String valuePlaceholder = ":value" + index;

            names.put(namePlaceholder, attributeName);
            values.put(valuePlaceholder, toAttributeValue(entry.getValue()));
            updateParts.add(namePlaceholder + " = " + valuePlaceholder);

            index++;
        }

        if (updateParts.isEmpty()) {
            return response(400, Map.of("error", "No updatable fields provided"));
        }

        UpdateItemRequest updateItemRequest = new UpdateItemRequest()
                .withTableName(tableName)
                .withKey(key)
                .withUpdateExpression("SET " + String.join(", ", updateParts))
                .withExpressionAttributeNames(names)
                .withExpressionAttributeValues(values)
                .withReturnValues(ReturnValue.ALL_NEW);

        UpdateItemResult result = dynamoDb.updateItem(updateItemRequest);

        return response(200, Map.of(
                "message", "Item updated successfully",
                "item", itemToMap(result.getAttributes())
        ));
    }

    private APIGatewayProxyResponseEvent deleteItem(String tableName, Map<String, AttributeValue> key) {
        dynamoDb.deleteItem(new DeleteItemRequest()
                .withTableName(tableName)
                .withKey(key));

        return response(200, Map.of("message", "Item deleted successfully"));
    }

    private Map<String, AttributeValue> key(String keyName, String keyValue) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put(keyName, new AttributeValue().withS(keyValue));
        return key;
    }

    private Map<String, Object> getBodyMap(APIGatewayProxyRequestEvent request) {
        String body = request.getBody();

        if (body == null || body.isBlank()) {
            return new HashMap<>();
        }

        if (Boolean.TRUE.equals(request.getIsBase64Encoded())) {
            body = new String(Base64.getDecoder().decode(body), StandardCharsets.UTF_8);
        }

        Map<String, Object> map = gson.fromJson(body, MAP_TYPE);

        if (map == null) {
            return new HashMap<>();
        }

        return map;
    }

    private Map<String, AttributeValue> jsonToItem(Map<String, Object> body) {
        Map<String, AttributeValue> item = new HashMap<>();

        for (Map.Entry<String, Object> entry : body.entrySet()) {
            item.put(entry.getKey(), toAttributeValue(entry.getValue()));
        }

        return item;
    }

    private AttributeValue toAttributeValue(Object value) {
        if (value == null) {
            return new AttributeValue().withNULL(true);
        }

        if (value instanceof Boolean boolValue) {
            return new AttributeValue().withBOOL(boolValue);
        }

        if (value instanceof Number numberValue) {
            BigDecimal number = new BigDecimal(numberValue.toString()).stripTrailingZeros();
            return new AttributeValue().withN(number.toPlainString());
        }

        return new AttributeValue().withS(String.valueOf(value));
    }

    private List<Map<String, Object>> itemsToList(List<Map<String, AttributeValue>> items) {
        List<Map<String, Object>> list = new ArrayList<>();

        for (Map<String, AttributeValue> item : items) {
            list.add(itemToMap(item));
        }

        return list;
    }

    private Map<String, Object> itemToMap(Map<String, AttributeValue> item) {
        Map<String, Object> map = new HashMap<>();

        for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
            AttributeValue value = entry.getValue();

            if (value.getS() != null) {
                map.put(entry.getKey(), value.getS());
            } else if (value.getN() != null) {
                map.put(entry.getKey(), value.getN());
            } else if (value.getBOOL() != null) {
                map.put(entry.getKey(), value.getBOOL());
            } else if (Boolean.TRUE.equals(value.getNULL())) {
                map.put(entry.getKey(), null);
            } else {
                map.put(entry.getKey(), value.toString());
            }
        }

        return map;
    }

    private String normalizePath(String path) {
        if (path == null || path.isBlank()) {
            return "/";
        }

        if (path.length() > 1 && path.endsWith("/")) {
            return path.substring(0, path.length() - 1);
        }

        return path;
    }

    private String getPathPart(String path, int index) {
        String[] parts = path.split("/");

        if (index >= parts.length) {
            return null;
        }

        return URLDecoder.decode(parts[index], StandardCharsets.UTF_8);
    }

    private String stringValue(Object value) {
        if (value == null) {
            return null;
        }

        return String.valueOf(value);
    }

    private APIGatewayProxyResponseEvent response(int statusCode, Object body) {
        Map<String, String> headers = new HashMap<>();
        headers.put("Access-Control-Allow-Origin", "*");
        headers.put("Access-Control-Allow-Headers", "Content-Type");
        headers.put("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS");

        return new APIGatewayProxyResponseEvent()
                .withStatusCode(statusCode)
                .withHeaders(headers)
                .withBody(gson.toJson(body));
    }
}