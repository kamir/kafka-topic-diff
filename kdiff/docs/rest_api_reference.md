# KDIFF REST API Reference

This document provides a comprehensive reference for the KDIFF REST API, which allows you to interact with KDIFF functionality through HTTP endpoints.

## API Overview

The KDIFF API is a RESTful service that allows you to:
- List and manage Kafka clusters
- List and read topics
- Compare topics and retrieve comparison results
- Manage configurations

## Base URL

The API is available at:

```
http://<hostname>:<port>/api/v1
```

Default port: 8080

## Authentication

The API supports the following authentication methods:

### Basic Authentication

```
Authorization: Basic <base64-encoded-credentials>
```

### Bearer Token (JWT)

```
Authorization: Bearer <jwt-token>
```

### API Key

```
X-API-Key: <api-key>
```

## Response Format

All responses are in JSON format with the following structure:

```json
{
  "status": "success|error",
  "data": { ... },  // Response data (if successful)
  "error": {        // Error details (if unsuccessful)
    "code": "ERROR_CODE",
    "message": "Human readable error message"
  }
}
```

## API Endpoints

### Cluster Management

#### List Clusters

Lists all configured Kafka clusters.

**Request:**
```
GET /clusters
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "clusters": [
      {
        "name": "production",
        "bootstrap_servers": "kafka1.example.com:9092,kafka2.example.com:9092",
        "security_protocol": "SASL_SSL"
      },
      {
        "name": "staging",
        "bootstrap_servers": "kafka-staging.example.com:9092",
        "security_protocol": "PLAINTEXT"
      }
    ]
  }
}
```

#### Get Cluster Details

Retrieves details for a specific cluster.

**Request:**
```
GET /clusters/{cluster_name}
```

**Path Parameters:**
- `cluster_name`: Name of the cluster to retrieve

**Response:**
```json
{
  "status": "success",
  "data": {
    "name": "production",
    "bootstrap_servers": "kafka1.example.com:9092,kafka2.example.com:9092",
    "security_protocol": "SASL_SSL",
    "sasl_mechanism": "PLAIN",
    "config_file": "production.properties"
  }
}
```

#### Add Cluster

Adds a new Kafka cluster configuration.

**Request:**
```
POST /clusters
Content-Type: application/json

{
  "name": "development",
  "properties": {
    "bootstrap.servers": "kafka-dev.example.com:9092",
    "security.protocol": "PLAINTEXT"
  }
}
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "name": "development",
    "message": "Cluster configuration added successfully"
  }
}
```

#### Update Cluster

Updates an existing cluster configuration.

**Request:**
```
PUT /clusters/{cluster_name}
Content-Type: application/json

{
  "properties": {
    "bootstrap.servers": "kafka-dev-new.example.com:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"password\";"
  }
}
```

**Path Parameters:**
- `cluster_name`: Name of the cluster to update

**Response:**
```json
{
  "status": "success",
  "data": {
    "name": "development",
    "message": "Cluster configuration updated successfully"
  }
}
```

#### Delete Cluster

Removes a cluster configuration.

**Request:**
```
DELETE /clusters/{cluster_name}
```

**Path Parameters:**
- `cluster_name`: Name of the cluster to delete

**Response:**
```json
{
  "status": "success",
  "data": {
    "message": "Cluster configuration removed successfully"
  }
}
```

### Topic Operations

#### List Topics

Lists topics available on a cluster.

**Request:**
```
GET /clusters/{cluster_name}/topics
```

**Path Parameters:**
- `cluster_name`: Name of the cluster

**Query Parameters:**
- `pattern` (optional): Regex pattern to filter topics

**Response:**
```json
{
  "status": "success",
  "data": {
    "topics": [
      "orders",
      "payments",
      "users",
      "events"
    ]
  }
}
```

#### Get Topic Information

Retrieves information about a specific topic.

**Request:**
```
GET /clusters/{cluster_name}/topics/{topic_name}
```

**Path Parameters:**
- `cluster_name`: Name of the cluster
- `topic_name`: Name of the topic

**Response:**
```json
{
  "status": "success",
  "data": {
    "name": "orders",
    "partitions": 12,
    "replication_factor": 3,
    "configs": {
      "cleanup.policy": "delete",
      "retention.ms": "604800000"
    }
  }
}
```

#### Read Messages

Reads messages from a topic.

**Request:**
```
GET /clusters/{cluster_name}/topics/{topic_name}/messages
```

**Path Parameters:**
- `cluster_name`: Name of the cluster
- `topic_name`: Name of the topic

**Query Parameters:**
- `start_offset` (optional): Starting offset
- `max_messages` (optional): Maximum number of messages to read
- `start_timestamp` (optional): Starting timestamp (in milliseconds)
- `partition` (optional): Specific partition to read from

**Response:**
```json
{
  "status": "success",
  "data": {
    "messages": [
      {
        "offset": 1000,
        "timestamp": 1609459200000,
        "key": "user123",
        "value": {
          "id": "order456",
          "amount": 99.95,
          "status": "completed"
        },
        "partition": 0,
        "headers": {
          "source": "web",
          "client-id": "browser-123"
        }
      },
      // ... more messages
    ],
    "count": 10,
    "has_more": true
  }
}
```

### Comparison Operations

#### Start Comparison

Initiates a comparison between topics.

**Request:**
```
POST /comparisons
Content-Type: application/json

{
  "cluster_a": "production",
  "topic_a": "orders",
  "cluster_b": "staging",
  "topic_b": "orders",
  "options": {
    "strategy": "table",
    "compare_content": true,
    "compare_offsets": false,
    "compare_timestamps": true,
    "timestamp_tolerance": 5000,
    "compare_schema_ids": true,
    "max_messages": 10000,
    "max_differences": 100,
    "start_timestamp_a": 1609459200000,
    "start_timestamp_b": 1609459200000
  }
}
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "comparison_id": "comp-123456",
    "status": "running",
    "estimated_completion": "2025-04-30T15:30:00Z"
  }
}
```

#### Get Comparison Status

Checks the status of a running comparison.

**Request:**
```
GET /comparisons/{comparison_id}
```

**Path Parameters:**
- `comparison_id`: ID of the comparison to check

**Response:**
```json
{
  "status": "success",
  "data": {
    "comparison_id": "comp-123456",
    "status": "running",
    "progress": 45.5,
    "messages_processed": 4550,
    "total_messages": 10000,
    "estimated_completion": "2025-04-30T15:30:00Z"
  }
}
```

#### Get Comparison Results

Retrieves the results of a completed comparison.

**Request:**
```
GET /comparisons/{comparison_id}/results
```

**Path Parameters:**
- `comparison_id`: ID of the comparison

**Query Parameters:**
- `page` (optional): Page number for paginated results
- `page_size` (optional): Number of differences per page
- `filter` (optional): Filter differences by type (content, offset, timestamp, schema_id)

**Response:**
```json
{
  "status": "success",
  "data": {
    "comparison_id": "comp-123456",
    "topic_a": "orders",
    "topic_b": "orders",
    "cluster_a": "production",
    "cluster_b": "staging",
    "summary": {
      "messages_compared": 10000,
      "differences_found": 5,
      "content_differences": 3,
      "offset_differences": 0,
      "timestamp_differences": 2,
      "schema_id_differences": 0,
      "missing_in_a": 0,
      "missing_in_b": 0,
      "elapsed_seconds": 35.2,
      "messages_per_second": 284.1
    },
    "differences": [
      {
        "type": "content",
        "message_a": {
          "offset": 1050,
          "key": "order123",
          "value_digest": "a1b2c3d4e5f6",
          "content_preview": "{\"id\":\"order123\",\"amount\":99.95,\"status\":\"completed\"}"
        },
        "message_b": {
          "offset": 1050,
          "key": "order123",
          "value_digest": "f6e5d4c3b2a1",
          "content_preview": "{\"id\":\"order123\",\"amount\":99.95,\"status\":\"pending\"}"
        },
        "details": "Field 'status' differs: \"completed\" vs \"pending\""
      },
      // ... more differences
    ],
    "pagination": {
      "page": 1,
      "page_size": 10,
      "total_pages": 1,
      "total_items": 5
    }
  }
}
```

#### Cancel Comparison

Cancels a running comparison.

**Request:**
```
DELETE /comparisons/{comparison_id}
```

**Path Parameters:**
- `comparison_id`: ID of the comparison to cancel

**Response:**
```json
{
  "status": "success",
  "data": {
    "message": "Comparison canceled successfully"
  }
}
```

### Configuration Management

#### Get Global Configuration

Retrieves the global KDIFF configuration.

**Request:**
```
GET /config
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "default": {
      "strategy": "stream",
      "output_format": "text",
      "checksum_algorithm": "SHA256",
      "comparison": {
        "content": true,
        "offsets": false,
        "timestamps": false,
        "schema_ids": false
      },
      "max_messages": 1000
    }
  }
}
```

#### Update Global Configuration

Updates the global KDIFF configuration.

**Request:**
```
PUT /config
Content-Type: application/json

{
  "default": {
    "strategy": "table",
    "output_format": "json",
    "checksum_algorithm": "SHA512",
    "comparison": {
      "content": true,
      "offsets": true,
      "timestamps": true,
      "schema_ids": true
    },
    "max_messages": 5000
  }
}
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "message": "Configuration updated successfully"
  }
}
```

## Error Codes

| Code | Description |
|------|-------------|
| `INVALID_REQUEST` | The request is malformed or contains invalid parameters |
| `CLUSTER_NOT_FOUND` | The specified cluster does not exist |
| `TOPIC_NOT_FOUND` | The specified topic does not exist |
| `COMPARISON_NOT_FOUND` | The specified comparison ID does not exist |
| `AUTHENTICATION_ERROR` | Authentication failed |
| `AUTHORIZATION_ERROR` | Not authorized to access the resource |
| `KAFKA_CONNECTION_ERROR` | Failed to connect to Kafka cluster |
| `INTERNAL_ERROR` | An unexpected error occurred |

## Example Usage

### Compare Two Topics

```bash
# 1. Start a comparison
curl -X POST http://localhost:8080/api/v1/comparisons \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your-token" \
  -d '{
    "cluster_a": "production",
    "topic_a": "orders",
    "cluster_b": "staging",
    "topic_b": "orders",
    "options": {
      "strategy": "table",
      "compare_content": true,
      "max_messages": 1000
    }
  }'

# Response with comparison_id
# {"status":"success","data":{"comparison_id":"comp-123456","status":"running"}}

# 2. Check comparison status
curl -X GET http://localhost:8080/api/v1/comparisons/comp-123456 \
  -H "Authorization: Bearer your-token"

# 3. Get results when complete
curl -X GET http://localhost:8080/api/v1/comparisons/comp-123456/results \
  -H "Authorization: Bearer your-token"
```

## Pagination

For endpoints that may return large datasets, pagination is supported through the following query parameters:

- `page`: Page number (1-based)
- `page_size`: Number of items per page

Response will include pagination information:

```json
"pagination": {
  "page": 1,
  "page_size": 10,
  "total_pages": 5,
  "total_items": 42
}
```

## Rate Limiting

The API implements rate limiting to ensure service stability:

- 100 requests per minute per API key
- 20 requests per minute for unauthenticated users

When rate limit is exceeded, the API responds with:

```
HTTP/1.1 429 Too Many Requests
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 0
X-RateLimit-Reset: 1609459260

{
  "status": "error",
  "error": {
    "code": "RATE_LIMIT_EXCEEDED",
    "message": "Rate limit exceeded. Please try again in 60 seconds."
  }
}
```

## Versioning

The API uses URL versioning (e.g., `/api/v1/`). Breaking changes will be introduced in new API versions.

## Webhook Notifications

You can register webhooks to receive notifications when comparisons complete:

**Request:**
```
POST /webhooks
Content-Type: application/json

{
  "url": "https://example.com/callback",
  "events": ["comparison.completed", "comparison.failed"],
  "secret": "your-webhook-secret"
}
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "webhook_id": "wh-123456",
    "url": "https://example.com/callback",
    "events": ["comparison.completed", "comparison.failed"]
  }
}
```

When a comparison is complete, the service will POST to your webhook URL:

```json
{
  "event": "comparison.completed",
  "timestamp": "2025-04-30T15:30:00Z",
  "data": {
    "comparison_id": "comp-123456",
    "topic_a": "orders",
    "topic_b": "orders",
    "summary": {
      "messages_compared": 10000,
      "differences_found": 5
    },
    "results_url": "http://localhost:8080/api/v1/comparisons/comp-123456/results"
  },
  "signature": "sha256=..." // HMAC signature using your secret
}
