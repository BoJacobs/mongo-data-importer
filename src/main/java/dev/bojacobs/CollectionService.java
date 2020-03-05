package dev.bojacobs;

import org.reflections.Reflections;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *  Service that is used to scan a package for classes that are annotated with @Document.
 * @author Bo Jacobs
 */
class CollectionService {

    private String entitiesBasePackage;

    public CollectionService(String entitiesBasePackage) {
        this.entitiesBasePackage = entitiesBasePackage;
    }

    /**
     *  Scans the specified package for classes that are annotated with @Document.
     *  Filters the found classes on the premise that the @Document annotation has a value for the 'collection' property.
     * @return List of collection names.
     */
    public List<String> fetchCollections() {
        Reflections reflections = new Reflections(this.entitiesBasePackage);
        Set<Class<?>> collectionClasses = reflections.getTypesAnnotatedWith(Document.class);

        return collectionClasses.stream()
                .map(c -> c.getAnnotation(Document.class).collection())
                .filter(StringUtils::hasText)
                .collect(Collectors.toList());
    }
}
