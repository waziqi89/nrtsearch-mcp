package com.yelp.nrtsearch.nrtsearchmcp;

import com.yelp.nrtsearch.nrtsearchmcp.client.NrtsearchClient;
import org.springframework.ai.support.ToolCallbacks;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class NrtsearchMcpConfiguration {
    @Bean
    public List<ToolCallback> tools(NrtsearchService nrtsearchService) {
        return List.of(
                ToolCallbacks.from(nrtsearchService)
        );
    }

    @Bean
    public NrtsearchClient nrtsearchClient() {
        return new NrtsearchClient("localhost", 6000);
    }
}
