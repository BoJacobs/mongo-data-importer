package dev.bojacobs.dao;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import dev.bojacobs.exception.MongoImporterException;
import org.springframework.util.StringUtils;

/**
 * @author Bo Jacobs
 */
public class ImportDao {

    private MongoDatabase mongoDatabase;
    private MongoClient mongoClient;

    public MongoDatabase connect(MongoClientURI mongoClientURI, String databaseName) throws MongoImporterException {
        MongoClient mongoClient = new MongoClient(mongoClientURI);
        String dbName = !StringUtils.hasText(databaseName) ? mongoClientURI.getDatabase() : databaseName;
        if(!StringUtils.hasText(dbName)) {
            throw new MongoImporterException("No database name in URI or passed as argument");
        }
        this.mongoClient = mongoClient;
        this.mongoDatabase = mongoClient.getDatabase(dbName);

        return this.mongoDatabase;
    }

    public void close() {
        this.mongoClient.close();
    }

    public MongoDatabase getMongoDatabase() {
        return this.mongoDatabase;
    }
}
