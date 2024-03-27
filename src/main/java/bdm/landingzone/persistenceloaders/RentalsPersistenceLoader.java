package bdm.landingzone.persistenceloaders;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.lang.reflect.Type;
import java.util.List;
import java.util.stream.Stream;

public class RentalsPersistenceLoader {

    private MongoCollection<Document> collection;

    public RentalsPersistenceLoader(String connectionString, String dbName, String collectionName) {
        MongoClient mongoClient = MongoClients.create(connectionString);
        MongoDatabase database = mongoClient.getDatabase(dbName);
        collection = database.getCollection(collectionName);
    }

    public void loadRentalsFromJsonFiles(String directoryPath) {
        try (Stream<java.nio.file.Path> paths = Files.walk(Paths.get(directoryPath))) {
            paths.filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(".json"))
                    .forEach(this::insertJsonFileIntoCollection);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void insertJsonFileIntoCollection(java.nio.file.Path filePath) {
        try {
            String jsonContent = new String(Files.readAllBytes(filePath));
            Gson gson = new Gson();
            Type listType = new TypeToken<List<Document>>(){}.getType();
            List<Document> documents = gson.fromJson(jsonContent, listType);
            if (! documents.isEmpty()) {
                // Extract the date from the filename and add it to each document
                String fileName = filePath.getFileName().toString();
                String date = fileName.substring(0, fileName.indexOf('.'));
                documents.forEach(doc -> doc.append("date", date));

                collection.insertMany(documents);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}