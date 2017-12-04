package weblog;

import com.mongodb.*;
import com.mongodb.client.*;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Iterator;

public class MongoDBConnection {
    private MongoClient mongo;
    private MongoDatabase database;
    private MongoCollection<Document> collection;

    public void MongoConnection(String databaseName, String collectionName){
        mongo = new MongoClient("localhost", 27017);
        database = mongo.getDatabase(databaseName);
        collection = database.getCollection(collectionName);
        System.out.println("connected");
    }

    public void insertToCollection(ArrayList<Weblog> weblogs){
        ArrayList<Document> documents = new ArrayList<>();
        for (Weblog weblog : weblogs) {
            Document log = new Document();
            log.put("URL", weblog.getURL());
            log.put("IP", weblog.getIP());
            log.put("timeStamp", weblog.getTimeStamp());
            log.put("timeSpent", weblog.getTimeSpent());
            documents.add(log);
        }
        collection.insertMany(documents);
    }

    public void printCollection(){
        MongoConnection("weblogs", "logs");
        Iterator<Document> query = collection.find().iterator();
        while(query.hasNext()) {
            System.out.println(query.next());
        }
        mongo.close();
    }
}
