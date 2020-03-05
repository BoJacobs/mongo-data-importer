# mongo-data-importer

A Spring tool that allows you to import data in MongoDB from data files on the classpath.

In the future, CSV support will be added.

## Getting started

Coming soon.

## Usage

To use the tool, you need to instantiate a MongoJSONImporter object.
Using Spring, you can instantiate the importer as a bean in the Spring context.

```java
@Bean
public MongoJSONImporter mongoJSONImporter(){
  MongoJSONImporter importer = new MongoJSONImporter("mongodb://DBHOST:DBPORT/DBNAME", new DefaultResourceLoader());

  return importer;
}
```

The tool will look for .json files in a 'data' directory on the classpath and create collections with the same name as the file. 

The JSON files should define an array of objects. Example:

```javascript
[
  {
    "name": "recordName",
    "anotherField": "anotherValue",
    "nestedObject": {
      "nestedField:" : 1
    }
  },
  {
    "name": "test2",
    "anotherField": "anotherValue",
    "arrayField": [
      "val1",
      "val2"
    ]
  },
  ...
]
```
