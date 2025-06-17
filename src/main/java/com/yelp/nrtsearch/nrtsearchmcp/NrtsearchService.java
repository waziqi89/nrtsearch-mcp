package com.yelp.nrtsearch.nrtsearchmcp;

import com.yelp.nrtsearch.nrtsearchmcp.client.NrtsearchClient;
import com.yelp.nrtsearch.server.grpc.IndicesRequest;
import com.yelp.nrtsearch.server.grpc.IndicesResponse;
import com.yelp.nrtsearch.server.grpc.StateRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class NrtsearchService {

    private static final Logger log = LoggerFactory.getLogger(NrtsearchService.class);
    @Autowired
    NrtsearchClient nrtsearchClient;

    @Tool(name = "list_indices", description = "List indices in the NRTSearch server. It returns a list of maps with index name (indexName), directory size (dirSize), and number of documents (numDocs). If an error occurs, it returns an empty list.")
    List<Map<String, String>> listIndices(){
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
}
