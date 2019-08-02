package at.hadl.logstatistics.utils.graphbuilding;

public enum QueryFeature {
    GROUP,
    OPTIONAL,
    UNION,
    FILTER,
    FILTER_EXISTS,
    FILTER_NOT_EXISTS,
    MINUS,
    SUB_QUERY,
    DATASET,
    SERVICE,
    NAMED_GRAPH,
    PROPERTY_PATH,
    VARIABLE_PREDICATE
}
