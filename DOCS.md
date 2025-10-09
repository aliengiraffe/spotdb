# SpotDB

## 🎯 Use Cases

SpotDB bridges the gap between AI assistants and data analysis by providing a **secure, ephemeral SQL sandbox** that can be accessed through the Model Context Protocol (MCP). This enables:

- **Safe AI Data Exploration**: LLMs can analyze your data without risk of accidental modification or exposure to production systems
- **Zero-Trust Architecture**: Every interaction is mediated through secure APIs with built-in validation and rate limiting
- **Instant Analytics**: Upload CSV files and immediately query them with SQL - no schema definition or database setup required
- **Privacy by Design**: Ephemeral storage means sensitive data is automatically destroyed when the session ends

## ✨ Features

### Core Capabilities

- **MCP Integration**: Native support for the Model Context Protocol enables AI assistants like Claude Desktop and Continue to directly query and analyze data through standardized MCP tools, providing seamless integration with AI workflows.

- **Secure Data Sandbox for LLMs & Automations**: Provides an isolated, read-only environment for LLMs and automated scripts to perform data analysis tasks. This prevents agents from accidentally writing to, deleting from, or otherwise corrupting production databases.

- **Guardrails for Safe Access**: The API acts as a secure intermediary. LLMs can only interact with data through predefined API endpoints that enforce security policies and data governance rules. This ensures that only safe, read-only queries are executed.

### Data Access & Management

- **HTTP API for CSV Uploads and Queries**: Easily upload CSV files and execute SQL queries using a simple HTTP interface. This makes it straightforward to integrate into web applications or automated workflows.

- **Unix Socket for Direct Database Access**: For low-latency, high-performance interactions, the container exposes a Unix socket. This allows direct communication with the Go API, bypassing the overhead of HTTP and enabling local applications to connect directly to the database.

- **Automated CSV Parsing and Table Creation**: Simply upload a CSV file, and the API will automatically parse it, infer data types, and create a corresponding table in the DuckDB instance. This eliminates the need for manual schema definition.

- **Database Snapshots with S3 Integration**: Create and restore complete database snapshots to/from Amazon S3. Load an initial database state at startup from S3, or create snapshots on-demand via the API for backup, versioning, or sharing database states across environments.

### Security & Performance

- **Built-in Security Validation**: Automatic detection and prevention of CSV injection attacks, with configurable handling modes (reject file, reject rows, or log warnings).

- **Rate Limiting**: Configurable per-IP rate limiting to prevent abuse and ensure fair resource usage across multiple clients.

- **Query Benchmarking**: Optional detailed performance metrics for queries including timing breakdowns, resource usage, and cache statistics.

- **Lightweight and Ephemeral**: The container is designed to be small and fast. It stores data only in-memory, making it ideal for transient workloads. All data is gone when the container stops, ensuring a clean slate for each run. This follows the principle of **ephemerality**, where resources are created and destroyed on demand.

### Technical Foundation

- **Built with Go and DuckDB**: The core logic is written in Go, a language known for its concurrency and performance, while DuckDB provides an in-process, high-speed analytical database engine. This combination ensures efficient data handling and query execution.

- **Thread-Safe Architecture**: Designed for concurrent operations in multi-user environments with proper connection pooling and resource management.

- **Graceful Shutdown**: Proper lifecycle management ensures clean termination and resource cleanup.

## 🏗️ Implementation Details

- Built with Go 1.25.x
- Uses github.com/marcboeker/go-duckdb for DuckDB integration
- Uses github.com/gin-gonic/gin for HTTP routing
- Development workflow with hot reloading via github.com/cosmtrek/air
- Implements proper error handling and graceful shutdown
- Manages DuckDB lifecycle and connection pooling
- Provides concurrent access to database via socket and HTTP
- Handles large file uploads and database operations efficiently
- Includes LLM-assisted schema detection for complex CSV files
- Thread-safe design for concurrent operations in multi-user environments
- Tiered approach to CSV importing with progressive fallback mechanisms

## 🕹️ Quick Start

### Basic Setup

> ⚠️ **Note**: The brew package will be available by mid-October. In the meantime, you can build from source using Go (see [Development Setup](#development-setup)).

1. Tap the repository:

```bash
brew tap aliengiraffe/spaceship
```

2. Install the package:

```bash
brew install spotdb
```

3. Start the server:

```bash
spotdb
```

4. Upload a CSV file:

```bash
curl -X POST -F "file=@data.csv" http://localhost:8080/upload
```

5. Query the data:

```bash
curl -X POST \
  http://localhost:8080/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM mytable LIMIT 10"}'
```

6. Stop the server:

```bash
spotdb stop
```

### Setting up Claude Desktop with spotdb MCP

To enable Claude Desktop to analyze your data using spotdb, follow these steps:

1. **Install spotdb and dependencies**:

```bash
# Install spotdb
brew tap aliengiraffe/spaceship
brew install spotdb

# Ensure npx is available (comes with Node.js)
# If not installed, install Node.js first:
# brew install node
```

1. **Start spotdb server**:

```bash
spotdb
# By default, runs on port 8080 with MCP SSE and Streamable HTTP endpoint
# at port 8081
```

1. **Configure Claude Desktop**:

   Open your Claude Desktop configuration file:

   ```bash
   # On macOS:
   open ~/Library/Application\ Support/Claude/claude_desktop_config.json

   # Or edit directly:
   nano ~/Library/Application\ Support/Claude/claude_desktop_config.json
   ```

1. **Add the spotdb MCP server** to the configuration:

   ```json
   {
     "mcpServers": {
       "spotdb-sse": {
         "command": "npx",
         "args": [
           "-y",
           "mcp-remote",
           "http://localhost:8081/sse",
           "--allow-http"
         ],
         "env": {
           "npm_config_yes": "true"
         }
       },
       "spotdb-streamable": {
         "command": "npx",
         "args": [
           "-y",
           "mcp-remote",
           "http://localhost:8081/stream",
           "--allow-http"
         ],
         "env": {
           "npm_config_yes": "true"
         }
       }
     }
   }
   ```

   Note: The `mcp-remote` package will be automatically installed via npx
   when Claude Desktop starts.

1. **Restart Claude Desktop** to load the new configuration.

1. **Upload CSV files using the web interface**:

   Since MCP doesn't support direct file uploads, you'll need to upload your CSV files through the web interface:

   ```bash
   # Open the web interface in your browser
   open http://localhost:8080/explorer
   ```

   In the web interface, you can:

   - Upload CSV files with automatic table creation
   - View available tables

1. **Start using spotdb in Claude**:
   - Ask Claude to analyze your uploaded data using SQL queries
   - Example prompts:
     - "Query the sales_data table and show me the top 10 customers by revenue"
     - "Create a summary of monthly trends from the uploaded data"
     - "Find outliers in the price column and explain what might cause them"

The MCP integration provides Claude with direct access to:

- Execute SQL queries on your uploaded data
- List available tables and their schemas
- Perform complex data analysis
- Generate insights and visualizations

All data remains ephemeral and secure - when you close Claude or stop spotdb, all uploaded data is automatically destroyed.

## Configuration

#### Environment Variables

The application can be configured using the following environment variables:

| Variable                   | Description                                                                          | Default Value      |
| -------------------------- | ------------------------------------------------------------------------------------ | ------------------ |
| `SOCKET_PORT`              | Port for WebSocket connections                                                       | `6033`             |
| `ENV_MAX_FILE_SIZE`        | Maximum file size for uploads in bytes                                               | `1073741824` (1GB) |
| `ENV_BUFFER_SIZE`          | Buffer size for file copying operations in bytes                                     | `65536` (64KB)     |
| `ENV_FILE_VALIDATION_MODE` | CSV content validation mode: `reject_file`, `reject_row`, `ignore`                   | `reject_file`      |
| `ENABLE_QUERY_BENCHMARKS`  | Enable detailed benchmarking for all queries                                         | `false`            |
| `ENV_RATE_LIMIT_RPS`       | Requests per second allowed for API rate limiting                                    | `5`                |
| `ENV_SERVER_MODE`          | Gin server mode (`debug`, `release`, or `test`)                                      | `release`          |
| `SNAPSHOT_LOCATION`        | S3 URI to load initial database snapshot from (e.g., `s3://bucket/path/snapshot.db`) | _(none)_           |
| `AWS_ACCESS_KEY_ID`        | AWS access key for S3 snapshot operations                                            | _(none)_           |
| `AWS_SECRET_ACCESS_KEY`    | AWS secret key for S3 snapshot operations                                            | _(none)_           |
| `AWS_REGION`               | AWS region for S3 snapshot operations                                                | _(none)_           |

When using Task runner, these values are set in the Taskfile.yml:

```bash
# Max file size is set to 2GB
# Buffer size is set to 64KB
task run              # Run with hot reload
task run-simple       # Run without hot reload
task docker           # Run in Docker container
```

You can override these values by setting the environment variables directly:

```bash
ENV_MAX_FILE_SIZE=536870912 ENV_BUFFER_SIZE=65536 task run-simple  # 512MB max size, 64KB buffer
```

## Development

### Codebase Setup

1. Install go-task:

   ```bash
   # macOS with Homebrew
   brew install go-task/tap/go-task

   # Or with go install
   go install github.com/go-task/task/v3/cmd/task@latest
   ```

1. Install goimports:

   ```bash
   # macOS with Homebrew
   brew install goimports

   # Or with go install
   go install golang.org/x/tools/cmd/goimports@latest
   ```

1. Install pre-commit hooks:

   ```bash
   # Install pre-commit using Homebrew
   brew install pre-commit

   # Install the hooks
   pre-commit install
   ```

### Development Setup

This project uses [go-task](https://taskfile.dev) for build automation.

Run development server with hot reloading:

```bash
# Start the server with hot reload
task run

# Optionally specify a custom socket port
task run SOCKET_PORT=8000
```

The application will automatically restart when code changes are detected.

To run without hot reloading:

```bash
task run-simple
```

Available tasks:

- `task -l` - List all available tasks
- `task build` - Build the binary
- `task test` - Run tests
- `task lint` - Run linter
- `task docker-build` - Build Docker image
- `task docker` - Run in Docker container (builds image first)
- `task swagger` - Auto generates swagger spec file based on code annotations
- `task` - Run the default task (build, test, run)

> **Note:** By default, tasks set environment variables for file upload size
> (2GB) and buffer size (64KB). See the [Configuration](#configuration)
> section for more information.

## Web Interface

SpotDB includes a built-in web interface accessible at `/explorer` that provides a user-friendly way to interact with your data without writing code.

### Accessing the Explorer UI

Once SpotDB is running, open your browser and navigate to:

```
http://localhost:8080/explorer
```

### Features

The Explorer UI provides the following capabilities:

#### 1. CSV File Upload

- **Drag-and-drop interface**: Simply drag your CSV file onto the upload area or click to browse
- **Auto-generated table names**: The UI automatically suggests table names based on your filename
- **File validation**: Validates file size (up to 2GB) and format before upload
- **Smart import**: Uses intelligent schema detection to handle complex CSV files
- **Real-time feedback**: Shows upload progress and displays success/error messages with detailed information

#### 2. Table Management

- **Live table listing**: View all available tables in your database
- **Schema inspection**: Click on any table to expand and view its complete schema, including:
  - Column names
  - Data types
  - Nullable constraints
- **Quick refresh**: Refresh the table list to see newly uploaded data
- **Responsive design**: Works seamlessly on desktop and mobile devices

#### 3. SQL Query Editor

- **Interactive query box**: Write and execute SQL queries directly in the browser
- **Keyboard shortcuts**: Use `Cmd+Enter` (Mac) or `Ctrl+Enter` (Windows/Linux) to execute queries
- **One-click query generation**: Click the "Query" button next to any table to auto-populate a sample SELECT query
- **Result visualization**: Query results are displayed in a clean, sortable table format
- **Performance metrics**: Shows row count and query execution time

#### 4. User Experience

- **Auto-complete**: Table names are automatically populated from uploaded filenames
- **Error handling**: Clear error messages with suggestions for resolution
- **Loading indicators**: Visual feedback during uploads and queries
- **Clean interface**: Modern, gradient-styled UI with smooth transitions and animations

### Example Workflow

1. Start SpotDB: `spotdb`
2. Open browser to `http://localhost:8080/explorer`
3. Drag a CSV file onto the upload area
4. Click "Upload CSV" to import the data
5. Click the expand icon (▶) next to your table to view its schema
6. Click the "Query" button to auto-populate a sample query, or write your own
7. Press `Cmd+Enter` to execute the query and view results

The Explorer UI is particularly useful for:

- Quick data exploration without writing API calls
- Validating CSV imports before integrating with AI workflows
- Prototyping SQL queries before using them in production
- Sharing data analysis capabilities with non-technical team members

## Usage

### Using Docker

#### Using Docker Compose

```bash
# Build and start the container
docker-compose up -d

# Check the container status
docker-compose ps
```

#### Using Docker

```bash
# Build the image
docker build -t spotdb .

# Run the container
docker run -p 8080:8080 -v spotdb-socket:/tmp --name spotdb spotdb
```

### API Usage

#### Upload a CSV File

Create the file `bruno/spotdb/.env` which has the data path of patient data

```
PATIENT_DATA_PATH=Path to patient_data.csv
```

If you are setting up a dev environment, add the base url on the file as well

```
DEV_BASE_URL=https://example.com/prefix
```

#### Basic Upload

```bash
curl -X POST \
  http://localhost:8080/api/v1/upload \
  -F "table_name=mytable" \
  -F "has_header=true" \
  -F "csv_file=@/path/to/data.csv"
```

> The default maximum file size is 1GB, but this can be configured using the
> `ENV_MAX_FILE_SIZE` environment variable. When using Task runner,
> the limit is set to 2GB.

Response:

```json
{
  "status": "success",
  "table": "mytable",
  "columns": [...],
  "row_count": 1000,
  "import": {
    "import_method": "direct_import"
  }
}
```

#### CSV Security Validation Modes

By default, CSV files are validated for potential security issues such as
formula injections and XSS attacks. You can configure how the system handles
detected security issues using the `ENV_FILE_VALIDATION_MODE` environment variable:

- `reject_file` (default): Rejects the entire file if any security issue is detected
- `reject_row`: Skips rows with security issues but processes safe rows
- `ignore`: Logs security warnings but imports all content

Example with row-level validation:

```bash
# Set validation mode to reject individual rows with security issues
export ENV_FILE_VALIDATION_MODE=reject_row

# Upload the CSV with some rows containing potential security issues
curl -X POST \
  http://localhost:8080/api/v1/upload \
  -F "table_name=mytable" \
  -F "has_header=true" \
  -F "csv_file=@/path/to/data.csv"
```

This allows importing files where only some rows have security issues while
skipping those specific rows.

#### Execute a Query

```bash
curl -X POST \
  http://localhost:8080/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM mytable LIMIT 10"}'
```

Standard Response:

```json
{
  "columns": ["column1", "column2"],
  "duration_ms": 1,
  "results": [
    {"column1": "value1", "column2": 123},
    ...
  ],
  "row_count": 2,
  "status": "success"
}
```

#### Query with Benchmarking Metrics

You can enable detailed benchmarking metrics by either:

1. Setting the `ENABLE_QUERY_BENCHMARKS` environment variable to `true`
2. Adding the `benchmark=true` query parameter to the request

```bash
curl -X POST \
  "http://localhost:8080/api/v1/query?benchmark=true" \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM mytable LIMIT 10"}'
```

Response with Benchmarks:

```json
{
  "columns": ["column1", "column2"],
  "duration_ms": 1,
  "results": [
    {"column1": "value1", "column2": 123},
    ...
  ],
  "row_count": 2,
  "status": "success",
  "benchmark": {
    "timing": {
      "total_ms": 1,
      "parsing_ms": 0.2,
      "planning_ms": 0.3,
      "execution_ms": 0.4,
      "serialization_ms": 0.1
    },
    "resources": {
      "peak_memory_bytes": 1048576,
      "thread_count": 4,
      "cpu_time_ms": 2,
      "io_read_bytes": 32768,
      "io_write_bytes": 0
    },
    "query_stats": {
      "rows_processed": 1000,
      "rows_returned": 2,
      "operator_count": 3,
      "scan_count": 1
    },
    "cache": {
      "hit_count": 10,
      "miss_count": 2,
      "hit_ratio": 0.83
    }
  }
}
```

#### List Tables

```bash
curl -X GET http://localhost:8080/api/v1/tables
```

Response:

```json
{
  "tables": ["table1", "table2", "table3"]
}
```

#### Create Database Snapshot

Create a snapshot of the current database state and upload it to Amazon S3:

```bash
curl -X POST \
  http://localhost:8080/api/v1/snapshot \
  -H "Content-Type: application/json" \
  -d '{
    "bucket": "my-bucket",
    "key": "snapshots/"
  }'
```

Response:

```json
{
  "status": "success",
  "snapshot_uri": "s3://my-bucket/snapshots/snapshot-2025-10-02T14-30-45.db",
  "filename": "snapshot-2025-10-02T14-30-45.db"
}
```

The snapshot filename is automatically generated with a timestamp in the format `snapshot-YYYY-MM-DDTHH-MM-SS.db`.

**Requirements for snapshot operations:**

- AWS credentials must be configured via environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`)
- The application must have write permissions to the specified S3 bucket
- Snapshots preserve the complete database state including all tables, data, and schema

#### Load Database from Snapshot

To load a database snapshot at application startup, set the `SNAPSHOT_LOCATION` environment variable:

```bash
# Set AWS credentials and snapshot location
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_REGION="us-west-1"
export SNAPSHOT_LOCATION="s3://my-bucket/snapshots/snapshot-2025-10-02T14-30-45.db"

# Start the application
spotdb
```

**Important notes:**

- If `SNAPSHOT_LOCATION` is set, the application will download and load the snapshot before starting
- The application will **fail to start** if the snapshot cannot be downloaded or loaded
- The snapshot replaces any existing database state
- This is useful for:
  - Sharing database states across team members
  - Restoring backups
  - Initializing development/testing environments with predefined data
  - Version control for database schemas and test data

## API Rate Limiting

The API has built-in rate limiting to protect against excessive requests:

- Default limit is 5 requests per second per IP address
- Rate limit violations return a 429 status code with a message indicating
  when to retry
- All rate limit violations are logged with the client IP and reset time
- Configure using environment variables:
  - `ENV_RATE_LIMIT_RPS=10` to change the requests-per-second limit (default: 5)

Rate limiting is automatically disabled in test mode or when
`ENV_RATE_LIMIT_RPS` is set to "0".

## Key Contributors

- [@aotarola](https://github.com/aotarola) - Core development and architecture
- [@nicobistolfi](https://github.com/nicobistolfi) - Core development and architecture

## Socket Connection

The container exposes a Unix socket at `/tmp/duckdb.sock` that can be used for
direct database access. The socket accepts JSON-formatted requests and returns
JSON-formatted responses.

Example socket request:

```json
{
  "type": "query",
  "query": "SELECT * FROM mytable LIMIT 10"
}
```

Example socket response:

```json
{
  "status": "success",
  "results": [
    {"column1": "value1", "column2": 123},
    ...
  ]
}
```
