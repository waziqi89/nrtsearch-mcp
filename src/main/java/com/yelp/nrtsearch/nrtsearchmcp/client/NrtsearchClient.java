package com.yelp.nrtsearch.nrtsearchmcp.client;


import com.yelp.nrtsearch.server.grpc.IndicesRequest;
import com.yelp.nrtsearch.server.grpc.IndicesResponse;
import com.yelp.nrtsearch.server.grpc.LuceneServerGrpc;
import com.yelp.nrtsearch.server.grpc.LuceneServerStubBuilder;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.StateRequest;
import com.yelp.nrtsearch.server.grpc.StateResponse;


public class NrtsearchClient {

    private final LuceneServerStubBuilder luceneServerStubBuilder;
    private final LuceneServerGrpc.LuceneServerBlockingStub blockingStub;

    public NrtsearchClient(String host, int port) {
            this.luceneServerStubBuilder =
                    new LuceneServerStubBuilder(host, port);
            this.blockingStub = luceneServerStubBuilder.createBlockingStub();
    }

    public SearchResponse search(SearchRequest searchRequest) {
        return blockingStub.search(searchRequest);
    }

    public IndicesResponse indices(IndicesRequest indicesRequest) {
        return blockingStub.indices(indicesRequest);
    }

    public StateResponse state(StateRequest stateRequest) {
        return blockingStub.state(stateRequest);
    }


}
