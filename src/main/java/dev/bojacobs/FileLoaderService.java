package dev.bojacobs;

import dev.bojacobs.exception.MongoImporterException;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.support.ResourcePatternUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *  Service that is used to load all files from the data directory on the classpath.
 * @author Bo Jacobs
 */
class FileLoaderService {

    private ResourceLoader resourceLoader;

    public FileLoaderService(ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
    }

    /**
     *  Finds all resources under the data directory on the classpath with the provided extension, adds them to map with their filename as key.
     * @return A Map containing Files, with key String.
     * @throws MongoImporterException
     */
    public Map<String, File> fetchAllFilesOfType(String extension) throws MongoImporterException {
        List<File> dataFiles = new ArrayList<>();
        try {
            Resource[] dataResources = ResourcePatternUtils.getResourcePatternResolver(resourceLoader).getResources("classpath*:data/*." + extension);
            for (Resource dataResource : dataResources) {
                File file = dataResource.getFile();
                dataFiles.add(file);
            }
        } catch (IOException e) {
            throw new MongoImporterException("Exception occurred while reading data files", e);
        }

        return dataFiles.stream().collect(Collectors.toMap(f -> f.getName().substring(0, f.getName().indexOf('.')), f -> f));
    }
}

