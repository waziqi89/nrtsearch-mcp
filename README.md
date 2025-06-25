To start the server:
java -jar ./build/libs/nrtsearch-mcp-0.0.1.jar

For Calude desktop configs:
`
{
  "mcpServers": {
    "nrtsearch": {
      "command": "java",
      "args": [
        "-jar",
        "-Dspring.profiles.active=stdio",
        "./build/libs/nrtsearch-mcp-0.0.1.jar"
      ]
    }
  }
}`

To inspect the server:
npx @modelcontextprotocol/inspector java -Dspring.profiles.active=stdio -jar ./build/libs/nrtsearch-mcp-0.0.1.jar