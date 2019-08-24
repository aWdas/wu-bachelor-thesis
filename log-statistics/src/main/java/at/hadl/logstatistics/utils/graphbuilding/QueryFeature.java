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
    SERVICE,
    NAMED_GRAPH,
    PROPERTY_PATH,
    VARIABLE_PREDICATE,
    NO_GRAPH_PATTERN,
    EMPTY_GRAPH_PATTERN,
    UNSUPPORTED_FEATURE
}
