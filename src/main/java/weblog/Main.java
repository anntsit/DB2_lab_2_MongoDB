package weblog;

import org.bson.Document;

import java.util.ArrayList;
import java.util.Iterator;

public class Main {
    public static void main(String[] args) {
        MongoDBConnection connection = new MongoDBConnection("weblogs", "logs");
        printQuery(connection.getAll());
        //printQuery(connection.findSortedIPbyURL("https://ru.wikipedia.org"));
        //printQuery(connection.findSortedURLbuIP("192.0.2.235"));
        //printQuery(connection.findSortedURLbuTimestamp("2017-11-05T22:15:00Z","2017-11-06T22:17:00Z"));
        //printQuery(connection.findTotalDurationOfURL());
        //printQuery(connection.findTimesOfVisitesToURL());
        printQuery(connection.findIPSumAndDuration());
    }

    public static void printQuery(Iterator<Document> query){
        while (query.hasNext()) {
            System.out.println(query.next());
        }
    }
}
