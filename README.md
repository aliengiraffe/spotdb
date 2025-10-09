# SpotDB

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=aliengiraffe_env&metric=alert_status&token=a564584b18c4d708580b4825a1d8c270b18a3f3f)](https://sonarcloud.io/summary/new_code?id=aliengiraffe_env)

### Lightweight data sandbox for AI workflows and data exploration, enabled with guardrails and security to keep your data safe.

This project provides a lightweight, **ephemeral data sandbox** designed for large language models (LLMs) and agentic workflows. By providing a secure, isolated environment, it allows AI agents and scripts to analyze data without direct access to production databases. This setup prevents accidental data modification, ensures data privacy, and enforces guardrails for safe data exploration.

## Features

- 🏖️ **Ephemeral Data Sandbox**: Create temporary databases for AI workflows and data exploration.
- 📸 **Snapshot**: Capture and store data snapshots, recover point-in-time data states or continue from a previous state.
- 🧠 **MCP API**: Access data through a Model Context Protocol for seamless integration with AI models and agentic workflows.
- ⚙️ **REST API**: Access data through a RESTful API for integration with traditional systems and workflows.
- 🚂 **Guardrails**: Enforce rules and constraints to ensure data safety and privacy.
- 🛡️ **Security**: Protect data from unauthorized access and modification.

## Quick Start

1. Tap the repository and install the package:

```bash
brew tap aliengiraffe/spaceship && \\
brew install spotdb
```

2. Start the server:

```bash
spotdb
```

3. Upload a CSV file:

```bash
curl -X POST \
  http://localhost:8080/api/v1/upload \
  -F "table_name=mytable" \
  -F "has_header=true" \
  -F "csv_file=@data.csv"
```

4. Query the data:

```bash
curl -X POST \
  http://localhost:8080/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM mytable LIMIT 10"}'
```

5. Setup Claude Code
   You must have the `claude` command installed.

Then, you can add the `spotdb` mcp:

```bash
claude mcp add spotdb -s user -- npx -y mcp-remote http://localhost:8081/stream
```

## Use Explorer UI

Open the Explorer UI in your browser and upload files and query the data:

```bash
open http://localhost:8080/explorer
```

## Full Documentation

👉 [https://github.com/aliengiraffe/spotdb/blob/main/DOCS.md](https://github.com/aliengiraffe/spotdb/blob/main/DOCS.md)
