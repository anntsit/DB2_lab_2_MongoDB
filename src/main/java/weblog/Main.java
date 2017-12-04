package weblog;

public class Main {
    public static void main(String[] args) {
        MongoDBConnection connection = new MongoDBConnection();
        connection.printCollection();
    }
}
