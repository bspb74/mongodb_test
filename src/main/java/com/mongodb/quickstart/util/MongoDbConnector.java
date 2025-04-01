package com.mongodb.quickstart.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.Block;
import com.mongodb.client.*;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;

import java.io.IOException;
import java.util.*;

public class MongoDbConnector {

    public static List<Map<String,Object>> rides = new ArrayList<>();

    public static void main(String[] args) {
        String connectionString = "mongodb://localhost:27017";
        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            System.out.println("=> Connection successful: " + preFlightChecks(mongoClient));
            System.out.println("=> Print list of databases:");
            List<Document> databases = mongoClient.listDatabases().into(new ArrayList<>());
            databases.forEach(db -> {
                System.out.println(db.toJson());
            });
            String db = "sample_aribnb";
            String db2 = "citibike_tripdata";
            String col = "locations";
            String col2 = "rides";
            MongoIterable<String> colls = mongoClient.getDatabase(db2).listCollectionNames();
            for (String c:colls) {
                System.out.println("Collection: " + c);
            }
            MongoCollection<Document> documents = mongoClient.getDatabase(db2).getCollection(col2);
            //Retrieving the documents
            for (Document d:documents.find()) {
                ;
                rides.add(convertJsonToMap(printJson(d)));
            }
        }
        System.out.println("Number of Rides: " + rides.size());
        rides.forEach(r -> {
            System.out.println("\n#-------------------------------------------------");
            for (Map.Entry m:r.entrySet()) {
                System.out.println(m.getKey().toString() + " => " + m.getValue().toString());
            }
        });
    }
    static String printJson(Document doc) {
        JsonWriterSettings prettyPrintSettings = JsonWriterSettings.builder()
                .indent(true)
                .build();

        // Convert the document to a pretty-printed JSON string
        String prettyJson = doc.toJson(prettyPrintSettings);

        // Print the pretty-printed JSON
//        System.out.println(prettyJson);
        return prettyJson;
    }

    static boolean preFlightChecks(MongoClient mongoClient) {
        Document pingCommand = new Document("ping", 1);
        Document response = mongoClient.getDatabase("admin").runCommand(pingCommand);
        System.out.println("=> Print result of the '{ping: 1}' command.");
        System.out.println(response.toJson(JsonWriterSettings.builder().indent(true).build()));
        return response.get("ok", Number.class).intValue() == 1;
    }

    public static Map<String, Object> convertJsonToMap(String jsonString) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(jsonString, new TypeReference<Map<String, Object>>() {
            });
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
