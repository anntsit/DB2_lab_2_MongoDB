package weblog;

import com.mongodb.*;
import com.mongodb.client.*;
import org.bson.Document;

import java.sql.Timestamp;
import java.text.*;
import java.util.*;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Indexes.descending;

public class MongoDBConnection {
    private MongoClient mongo;
    private MongoDatabase database;
    private MongoCollection<Document> collection;

    public MongoDBConnection(String databaseName, String collectionName){
        mongo = new MongoClient("localhost", 27017);
        database = mongo.getDatabase(databaseName);
        collection = database.getCollection(collectionName);
        System.out.println("CONNECTED");
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

    public Iterator<Document>  getAll(){
        return collection.find().iterator();
    }

    public Iterator<Document> findSortedIPbyURL(String url){
        return collection.find(new BasicDBObject("url", url)).sort(descending("IP")).iterator();
    }

    public Iterator<Document> findSortedURLbuIP(String ip){
        return collection.find(new BasicDBObject("ip", ip)).sort(descending("URL")).iterator();
    }

    public Iterator<Document> findSortedURLbuTimestamp(String startTime, String endTime){
        try{
        Date dateFromFormated = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(startTime);
        Date dateToFormated = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(endTime);
        Timestamp timeStampFrom = new Timestamp((dateFromFormated.getTime()));
        Timestamp timestampTo = new Timestamp((dateToFormated.getTime()));
            return collection.find(and(gt("timeStamp", timeStampFrom),
                    lt("timeStamp", timestampTo))).sort(descending("URL")).iterator();
    }
    catch (ParseException e) {
        System.out.println("Problem occurred while parsing date");
        return null;
    }
    }

    public Iterator<Document> findTotalDurationOfURL(){
        MongoCollection<Document> urlVisitDurationCollection =
                database.getCollection("url_visit_duration");
        String map = "function()" +
                "{ emit(this.URL,this.timeSpent); }";
        String reduce = "function(key, values)" +
                "{return Array.sum(values)}";

        collection.mapReduce(map, reduce).collectionName("url_visit_duration").toCollection();
        Iterator<Document> webLogs = urlVisitDurationCollection.find()
                .sort(descending("values")).iterator();
        return webLogs;
    }

    public Iterator<Document> findTimesOfVisitesToURL(){
        MongoCollection<Document> urlVisitSumCollection =
                database.getCollection("url_visit_sum");
        String map = "function()" +
                "{ emit(this.URL,1); }";
        String reduce = "function(key, values)" +
                "{var count = 0; for(var i in values)" +
                "{count += values[i];} " +
                "return count;}";
        collection.mapReduce(map, reduce).collectionName("url_visit_sum").toCollection();
        Iterator<Document> webLogs = urlVisitSumCollection.find()
                .sort(descending("value")).iterator();
        return webLogs;
    }

    public Iterator<Document> findURLVisitsPerPeriod(String dateFrom, String dateTo) {
        MongoCollection<Document> urlVisitPerPeriodCollection =
                database.getCollection("url_visit_per_period");
        String map = "function () {"
                + "if (this.timeStamp > ISODate"
                + "(\"" + dateFrom + "\")"
                + " && this.timeStamp < ISODate"
                + "(\"" + dateTo + "\"))"
                + " emit(this.url, 1); }";
        String reduce = "function(key, values) { return Array.sum(values); }";
        collection.mapReduce(map, reduce).collectionName("url_visit_per_period").toCollection();
        Iterator<Document> webLogs = urlVisitPerPeriodCollection.find()
                .sort(descending("value")).iterator();
        return webLogs;
    }

    public Iterator<Document> findIPSumAndDuration() {
        MongoCollection<Document> ipVisitSumDurationCollection =
                database.getCollection("ip_visit_sum_duration");
        String map = "function(){" +
                "values ={count:1,duration:this.timeSpent};" +
                "emit(this.IP,values);}";
        String reduce = "function(key, values) {" +
                "var count=0; var duration=0;" +
                "for(var i in values)" +
                "{count += values[i].count;" +
                "duration += values[i].duration;}" +
                "return {count:count, duration:duration}}";
        collection.mapReduce(map, reduce).collectionName("ip_visit_sum_duration").toCollection();
        Iterator<Document> webLogs = ipVisitSumDurationCollection.find()
                .sort(descending("count", "duration")).iterator();
        return webLogs;
    }
}
