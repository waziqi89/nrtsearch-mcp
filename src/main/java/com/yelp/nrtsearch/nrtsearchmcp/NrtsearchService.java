package com.yelp.nrtsearch.nrtsearchmcp;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.type.LatLng;
import com.yelp.nrtsearch.nrtsearchmcp.client.NrtsearchClient;
import com.yelp.nrtsearch.server.grpc.BooleanClause;
import com.yelp.nrtsearch.server.grpc.BooleanQuery;
import com.yelp.nrtsearch.server.grpc.GeoRadiusQuery;
import com.yelp.nrtsearch.server.grpc.IndicesRequest;
import com.yelp.nrtsearch.server.grpc.IndicesResponse;
import com.yelp.nrtsearch.server.grpc.MatchOperator;
import com.yelp.nrtsearch.server.grpc.MatchQuery;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.RangeQuery;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.TermInSetQuery;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@Service
public class NrtsearchService {

    private static final Logger log = LoggerFactory.getLogger(NrtsearchService.class);

    final NrtsearchClient nrtsearchClient;

    public NrtsearchService(@Autowired NrtsearchClient nrtsearchClient) {
        this.nrtsearchClient = nrtsearchClient;
    }

    @Tool(name = "list_indices", description = "List indices in the NRTSearch server. It returns a list of maps with index name (indexName), directory size (dirSize), and number of documents (numDocs). If an error occurs, it returns an empty list.")
    public List<Map<String, String>> listIndices() {
        try {
            IndicesResponse indicesResponse = nrtsearchClient.indices(IndicesRequest.newBuilder().build());
            return indicesResponse.getIndicesResponseList().stream()
                    .map(i -> Map.of("indexName", i.getIndexName(), "dirSize", Long.toString(i.getStatsResponse().getDirSize()), "numDocs", Integer.toString(i.getStatsResponse().getNumDocs())))
                    .toList();
        } catch (RuntimeException e) {
            log.error(e.getMessage());
            return Collections.emptyList();
        }
    }

    private static BooleanClause createMatchTextBooleanClause(String field, String strValue) {
        if (strValue == null || strValue.isEmpty()) {
            return null;
        }
        return BooleanClause.newBuilder()
                .setQuery(Query.newBuilder().setMatchQuery(MatchQuery.newBuilder().setField(field).setQuery(strValue).setOperator(MatchOperator.MUST)))
                .setOccur(BooleanClause.Occur.FILTER)
                .build();
    }

    private static BooleanClause createMatchExactBooleanClause(String field, Object value) {
        if (value == null) {
            return null;
        }

        String strValue = String.valueOf(value);
        if (strValue.isEmpty()) {
            return null;
        }
        if (strValue.contains(" ")) {
            // If the value contains spaces, treat it as a text match as this field is likely a text field analyzed/tokenized.
            return createMatchTextBooleanClause(field, strValue);
        }
        return BooleanClause.newBuilder()
                .setQuery(Query.newBuilder().setTermQuery(TermQuery.newBuilder().setField(field).setTextValue(strValue)))
                .setOccur(BooleanClause.Occur.FILTER)
                .build();
    }

    private static BooleanClause createMatchOneOfBooleanClause(String field, List<Object> value) {
        if (value == null) {
            return null;
        }
        List<String> strValues = value.stream()
                .map(Object::toString)
                .filter(str -> !str.isEmpty())
                .toList();
        if (strValues.isEmpty()) {
            return null;
        }
        return BooleanClause.newBuilder()
                .setQuery(Query.newBuilder().setTermInSetQuery(TermInSetQuery.newBuilder().setField(field).setTextTerms(TermInSetQuery.TextTerms.newBuilder().addAllTerms(strValues))))
                .setOccur(BooleanClause.Occur.FILTER)
                .build();
    }

    private static BooleanClause createMatchRangeBooleanClause(String field, Map<String, Double> range) {
        if (range == null || range.isEmpty()) {
            return null;
        }
        RangeQuery.Builder rangeQueryBuilder = RangeQuery.newBuilder().setField(field);
        if (range.containsKey("gt")) {
            rangeQueryBuilder.setLower(Double.toString(range.get("gt"))).setLowerExclusive(true);
        }
        if (range.containsKey("gte")) {
            rangeQueryBuilder.setLower(Double.toString(range.get("gte"))).setLowerExclusive(false);
        }
        if (range.containsKey("lt")) {
            rangeQueryBuilder.setUpper(Double.toString(range.get("lt"))).setUpperExclusive(true);
        }
        if (range.containsKey("lte")) {
            rangeQueryBuilder.setUpper(Double.toString(range.get("lte"))).setUpperExclusive(false);
        }
        return BooleanClause.newBuilder()
                .setQuery(Query.newBuilder().setRangeQuery(rangeQueryBuilder))
                .setOccur(BooleanClause.Occur.FILTER)
                .build();
    }

    private static BooleanClause createMatchGeoRadiusBooleanClause(String field, Map<String, Double> geoRadius) {
        if (geoRadius == null || !geoRadius.containsKey("lat") || !geoRadius.containsKey("lon") || !geoRadius.containsKey("radius")) {
            return null;
        }
        double lat = geoRadius.get("lat");
        double lon = geoRadius.get("lon");
        double radius = geoRadius.get("radius");

        return BooleanClause.newBuilder()
                .setQuery(Query.newBuilder().setGeoRadiusQuery(
                        GeoRadiusQuery.newBuilder()
                                .setField(field)
                                .setCenter(LatLng.newBuilder().setLatitude(lat).setLongitude(lon))
                                .setRadius(Double.toString(radius))))
                .setOccur(BooleanClause.Occur.FILTER)
                .build();
    }


    // TODO: support sorting
    @Tool(name = "search", description = "Search for documents in a specific index. It allows filtering by exact matches, text match,one-of matches, range queries, and geo-radius queries. It returns a json representation of search response contains hits and total hits count. If an error occurs, it returns null. Exact Match should be provided as a map with field names as keys and exact values as values. Text Match uses to do a partial phrase match. One-of Match should be provided as a map with field names as keys and lists of values as values. Range queries should be provided as a map with field names as keys and maps with 'gt'/'gte' and/or 'lt'/'lte' values. Geo-radius queries should be provided as a map with field names as keys and maps with 'lat', 'lon', and 'radius' values; Radius is in unit of meter. Retrieve fields should be provided as a list of field names to retrieve. If no fields are specified, all fields will be retrieved by providing a null argument.")
    public String search(String indexName, int size, List<String> retrieveFields,
                         Map<String, Object> matchExact,
                         Map<String, String> matchText,
                         Map<String, List<Object>> matchOneOf,
                         Map<String, Map<String, Double>> matchRange,
                         Map<String, Map<String, Double>> geoRaduisRange) {
        BooleanQuery.Builder booleanQueryBuilder = BooleanQuery.newBuilder();

        if (matchExact != null) {
            matchExact.forEach((field, value) -> {
                BooleanClause clause = createMatchExactBooleanClause(field, value);
                if (clause != null) {
                    booleanQueryBuilder.addClauses(clause);
                }
            });
        }

        if (matchText != null) {
            matchText.forEach((field, value) -> {
                BooleanClause clause = createMatchTextBooleanClause(field, value);
                if (clause != null) {
                    booleanQueryBuilder.addClauses(clause);
                }
            });
        }

        if (matchOneOf != null) {
            matchOneOf.forEach((field, value) -> {
                BooleanClause clause = createMatchOneOfBooleanClause(field, value);
                if (clause != null) {
                    booleanQueryBuilder.addClauses(clause);
                }
            });
        }

        if (matchRange != null) {
            matchRange.forEach((field, value) -> {
                BooleanClause clause = createMatchRangeBooleanClause(field, value);
                booleanQueryBuilder.addClauses(clause);
            });
        }

        if (geoRaduisRange != null) {
            geoRaduisRange.forEach((field, value) -> {
                BooleanClause clause = createMatchGeoRadiusBooleanClause(field, value);
                if (clause != null) {
                    booleanQueryBuilder.addClauses(clause);
                }
            });
        }

        SearchRequest searchRequest = SearchRequest.newBuilder()
                .setIndexName(indexName)
                .setQuery(Query.newBuilder().setBooleanQuery(booleanQueryBuilder))
                .setTopHits(size)
                .addAllRetrieveFields(retrieveFields == null ? List.of("*") : retrieveFields)
                .build();

        try {
            SearchResponse searchResponse = nrtsearchClient.search(searchRequest);
            return JsonFormat.printer().print(searchResponse);
        } catch (RuntimeException | InvalidProtocolBufferException e) {
            log.error(e.getMessage());
            return null;
        }
    }
}
