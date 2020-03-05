package dev.bojacobs;

import dev.bojacobs.dao.ImportDao;
import dev.bojacobs.exception.MongoImporterException;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.ResourceLoader;

import java.io.File;
import java.io.FileReader;
import java.util.List;
import java.util.Map;


/**
 * Class used for importing collections and records based on .json files that are in a /data
 * @author Bo Jacobs
 */
public class MongoJSONImporter implements InitializingBean {
    private static final Logger logger = LoggerFactory.getLogger(MongoJSONImporter.class);

    private ImportDao importDao;
    private JSONParser jsonParser;
    private FileLoaderService fileLoaderService;

    private String databaseName;
    private MongoClientURI mongoClientURI;
    private String entityScanPackage;

    private boolean allowUndefinedCollections;

    /**
     * Constructor for MongoJSONImporter
     * @param mongoClientURI The MongoClientUri used for instantiating a MongoClient
     * @param resourceLoader A resourceLoader used to load the data files. If null, the @DefaultResourceLoader is used
     */
    public MongoJSONImporter(MongoClientURI mongoClientURI, ResourceLoader resourceLoader) {
        this.importDao = new ImportDao();
        this.jsonParser = new JSONParser();
        if(resourceLoader == null) {
            resourceLoader = new DefaultResourceLoader();
        }
        this.fileLoaderService = new FileLoaderService(resourceLoader);
        this.mongoClientURI = mongoClientURI;
        this.databaseName = mongoClientURI.getDatabase();
        this.allowUndefinedCollections = true;
    }

    /**
     * Alternative constructor for MongoJSONImporter
     * @param mongoClientURI This string is used to build a MongoClientUri
     * @param resourceLoader resourceLoader used to load the data files. If null, the @DefaultResourceLoader is used
     */
    public MongoJSONImporter(String mongoClientURI, ResourceLoader resourceLoader) {
        this(new MongoClientURI(mongoClientURI), resourceLoader);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.startImport();
    }

    /**
     *  This method is used to set the package where the importer should look for entities (annotated with @Document)
     * @param entityScanPackage String reference to the package
     * @return returns the instance to allow config method chaining
     */
    public MongoJSONImporter setEntityScanPackage(String entityScanPackage) {
        this.entityScanPackage = entityScanPackage;
        return this;
    }

    /**
     *  This method is used to set the creation behaviour of the importer.
     *  If undefined collections are not allowed, collections are only created if a corresponding entity (annotated with @Document) exists.
     * @param allowUndefinedCollections
     * @return returns the instance to allow config method chaining
     */
    public MongoJSONImporter allowUndefinedCollections(boolean allowUndefinedCollections) {
        this.allowUndefinedCollections = allowUndefinedCollections;
        return this;
    }

    /**
     * Is called after bean properties are set and tries to open connection to the database
     * before starting the import process.
     * @throws MongoImporterException
     */
    private void startImport() throws MongoImporterException {
        try {
            logger.info("MongoImporter is connecting to the database...");
            this.importDao.connect(this.mongoClientURI, this.databaseName);
        } catch (Exception e) {
            throw new MongoImporterException("MongoImporter failed to connect to the database", e);
        }
        logger.info("MongoImporter is connected to the database.");

        importData();
    }

    /**
     * This method retrieves all allowed files from the data directory, parses them and creates collections accordingly.
     * @throws MongoImporterException
     */
    private void importData() throws MongoImporterException {
        Map<String, File> allowedDataFiles = this.fetchAllowedFiles();
        MongoDatabase mongoDatabase = importDao.getMongoDatabase();
        for (String dataFileName : allowedDataFiles.keySet()) {
            int counter = 0;
            MongoCollection<Document> collection = mongoDatabase.getCollection(dataFileName);
            try (FileReader fileReader = new FileReader(allowedDataFiles.get(dataFileName))) {
                JSONArray jsonArray = (JSONArray) jsonParser.parse(fileReader);
                for (Object o : jsonArray) {
                    JSONObject jsonObject = (JSONObject) o;
                    Document document = new Document();
                    for (Object key : jsonObject.keySet()) {
                        document.append((String) key, jsonObject.get(key));
                    }
                    collection.insertOne(document);
                    counter++;
                }
            } catch (Exception e) {
                importDao.close();
                throw new MongoImporterException("Error while parsing .json file.", e);
            }
            logger.info("Created collection '{}' and added {} records", dataFileName, counter);
        }

        importDao.close();
    }

    /**
     * Retrieves all files and will then, depending on configuration, filter files based on defined entities.
     * @return a Map of allowed files with their name as key.
     * @throws MongoImporterException
     */
    private Map<String, File> fetchAllowedFiles() throws MongoImporterException {
        logger.info("MongoImporter is fetching all .json files from the data directory...");
        Map<String, File> dataFiles = this.fileLoaderService.fetchAllFilesOfType("json");
        logger.info("MongoImporter fetched {} datafiles.", dataFiles.size());

        if (!allowUndefinedCollections) {
            CollectionService collectionService = new CollectionService(this.entityScanPackage);
            List<String> collectionNames = collectionService.fetchCollections();
            logger.info("MongoImporter found {} defined collections.", collectionNames.size());
            dataFiles.keySet().removeIf(key -> !collectionNames.contains(key));
        }

        return dataFiles;
    }
}
