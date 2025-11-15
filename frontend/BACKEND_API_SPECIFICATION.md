# Backend API Specification for AURAverse Frontend

This document describes the exact data formats that the frontend will send to the backend and expects to receive from the backend. Use this as a reference to implement your backend API correctly.

---

## Table of Contents

1. [Base URL Configuration](#base-url-configuration)
2. [Data Flow Overview](#data-flow-overview)
3. [Endpoint 1: Upload Files (Unified)](#endpoint-1-upload-files-unified)
4. [Endpoint 2: Get Database State](#endpoint-2-get-database-state)
5. [Endpoint 3: Get Database Entity (Optional)](#endpoint-3-get-database-entity-optional)
6. [Error Response Format](#error-response-format)
7. [Complete Data Structure Definitions](#complete-data-structure-definitions)

---

## Base URL Configuration

**Default Base URL:** `http://localhost:8000/api`

The frontend reads the base URL from environment variable `VITE_API_BASE_URL` or defaults to `http://localhost:8000/api`.

You can configure it by creating a `.env` file:
```
VITE_API_BASE_URL=http://localhost:8000/api
```

---

## Data Flow Overview

### Upload Flow:
1. User uploads files or folder via frontend UI (supports all file types)
2. Frontend calls unified upload endpoint (`/api/upload`)
3. Backend analyzes files, processes the data, saves it to database, and returns updated database state
4. Frontend updates visualization with the returned state

### Visualization Flow:
1. Frontend loads on page mount
2. Frontend calls `GET /api/database/state` to fetch current database structure
3. Backend returns current state (tables, collections, media directories)
4. Frontend displays the visualization

---

## Endpoint 1: Upload Files (Unified)

**Endpoint:** `POST /api/upload`

**Description:** Upload files and folders (any file type) along with optional metadata. Backend should analyze files, intelligently categorize them, decide storage format (SQL/NoSQL/Media), process, and store them.

---

### Request Format

**Method:** `POST`  
**Content-Type:** `multipart/form-data`  
**URL:** `{BASE_URL}/upload`

#### Form Data Fields:

| Field Name | Type | Required | Description |
|------------|------|----------|-------------|
| `files` | File[] | Yes | Array of File objects (any file type: images, videos, JSON, documents, etc.) |
| `metadata` | string | No | Optional user-provided metadata/comments |

#### Example Request (multipart/form-data):

```
POST /api/upload HTTP/1.1
Content-Type: multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW

------WebKitFormBoundary7MA4YWxkTrZu0gW
Content-Disposition: form-data; name="metadata"

User comments about these files
------WebKitFormBoundary7MA4YWxkTrZu0gW
Content-Disposition: form-data; name="files"; filename="photo1.jpg"
Content-Type: image/jpeg

[binary file data]
------WebKitFormBoundary7MA4YWxkTrZu0gW
Content-Disposition: form-data; name="files"; filename="data.json"
Content-Type: application/json

[binary file data]
------WebKitFormBoundary7MA4YWxkTrZu0gW
Content-Disposition: form-data; name="files"; filename="document.pdf"
Content-Type: application/pdf

[binary file data]
------WebKitFormBoundary7MA4YWxkTrZu0gW--
```

#### JavaScript Code (Frontend):
```javascript
// Frontend sends this:
const formData = new FormData()
if (metadata) {
  formData.append('metadata', metadata)  // Optional
}
files.forEach(file => {
  formData.append('files', file)  // File objects - any type
})
```

#### File Types Supported:
- **Images**: JPEG, PNG, GIF, WebP, etc.
- **Videos**: MP4, AVI, MOV, etc.
- **Documents**: JSON, TXT, PDF, DOC, DOCX, CSV, etc.
- **Any other file type** - Backend should handle intelligently

---

### Response Format

**Status Code:** `200 OK`  
**Content-Type:** `application/json`

#### Success Response Structure:

The backend should analyze all uploaded files and intelligently categorize them. The response should include the complete updated database state.

**Example 1: Mixed File Types (Images, JSON, Documents):**

```json
{
  "success": true,
  "message": "Files uploaded and processed successfully",
  "databaseState": {
    "tables": [
      {
        "name": "table_users",
        "type": "SQL",
        "columns": [
          { "name": "name", "type": "VARCHAR(255)" },
          { "name": "age", "type": "INTEGER" }
        ],
        "relationships": []
      }
    ],
    "collections": [
      {
        "name": "collection_logs",
        "count": 1,
        "schema": {
          "fileName": "logs.json",
          "timestamp": "string",
          "level": "string",
          "message": "string"
        }
      }
    ],
    "mediaDirectories": [
      {
        "name": "directory_1703123456789",
        "category": "Photos",
        "files": [
          {
            "name": "photo1.jpg",
            "size": 1024000,
            "type": "image/jpeg"
          },
          {
            "name": "photo2.jpg",
            "size": 2048000,
            "type": "image/jpeg"
          }
        ]
      }
    ]
  }
}
```

**Example 2: Folder Upload (Multiple File Types):**

```json
{
  "success": true,
  "message": "Folder uploaded and processed successfully",
  "databaseState": {
    "tables": [],
    "collections": [
      {
        "name": "collection_project_data",
        "count": 3,
        "schema": {
          "fileName": "data.json",
          "fileType": "application/json",
          "note": "Processed from folder upload"
        }
      }
    ],
    "mediaDirectories": [
      {
        "name": "directory_1703123456790",
        "category": "Media",
        "files": [
          {
            "name": "document.pdf",
            "size": 512000,
            "type": "application/pdf"
          },
          {
            "name": "image.png",
            "size": 2048000,
            "type": "image/png"
          }
        ]
      }
    ]
  }
}
```

#### Important Notes:

1. **`databaseState` is required** - The frontend expects this field to contain the updated database state
2. If `databaseState` is missing, the frontend will make a separate call to `GET /api/database/state`
3. Backend should intelligently analyze files:
   - **Images/Videos** → Store in `mediaDirectories` with appropriate category
   - **JSON files** → Parse and decide SQL vs NoSQL based on structure
   - **Other files** → Store metadata in collections or as appropriate
4. All three arrays (`tables`, `collections`, `mediaDirectories`) should reflect the current complete state
5. File objects should include `name`, `size` (in bytes), and `type` (MIME type)
6. When folders are uploaded, backend should preserve file paths/names and organize appropriately

---

## Endpoint 2: Get Database State

**Endpoint:** `GET /api/database/state`

**Description:** Fetch the current complete database visualization state. Called on page load and after uploads.

---

### Request Format

**Method:** `GET`  
**Headers:** `Content-Type: application/json`  
**URL:** `{BASE_URL}/database/state`  
**Query Parameters:** None

#### Example Request:

```
GET /api/database/state HTTP/1.1
Host: localhost:8000
Content-Type: application/json
```

---

### Response Format

**Status Code:** `200 OK`  
**Content-Type:** `application/json`

#### Success Response Structure:

```json
{
  "tables": [
    {
      "name": "table_users",
      "type": "SQL",
      "columns": [
        {
          "name": "id",
          "type": "INTEGER"
        },
        {
          "name": "name",
          "type": "VARCHAR(255)"
        },
        {
          "name": "email",
          "type": "VARCHAR(255)"
        }
      ],
      "relationships": [
        {
          "target": "table_orders",
          "type": "one-to-many"
        }
      ]
    },
    {
      "name": "table_products",
      "type": "SQL",
      "columns": [
        {
          "name": "id",
          "type": "INTEGER"
        },
        {
          "name": "name",
          "type": "VARCHAR(255)"
        },
        {
          "name": "price",
          "type": "DECIMAL(10,2)"
        }
      ],
      "relationships": []
    }
  ],
  "collections": [
    {
      "name": "collection_logs",
      "count": 150,
      "schema": {
        "timestamp": "string",
        "level": "string",
        "message": "string",
        "metadata": "object",
        "user": {
          "id": "number",
          "name": "string"
        }
      }
    },
    {
      "name": "collection_documents",
      "count": 45,
      "schema": {
        "title": "string",
        "content": "string",
        "tags": "array",
        "author": {
          "name": "string",
          "email": "string"
        }
      }
    }
  ],
  "mediaDirectories": [
    {
      "name": "directory_1703123456789",
      "category": "Photos",
      "files": [
        {
          "name": "photo1.jpg",
          "size": 1024000,
          "type": "image/jpeg"
        },
        {
          "name": "photo2.jpg",
          "size": 2048000,
          "type": "image/jpeg"
        }
      ]
    },
    {
      "name": "directory_1703123456790",
      "category": "Videos",
      "files": [
        {
          "name": "video1.mp4",
          "size": 10485760,
          "type": "video/mp4"
        }
      ]
    }
  ]
}
```

#### Important Notes:

1. **All three arrays are required** even if empty: `tables`, `collections`, `mediaDirectories`
2. Arrays can be empty `[]` if no data exists
3. This endpoint is called on page load to initialize the visualization
4. Return the complete current state of the database

---

## Endpoint 3: Get Database Entity (Optional)

**Endpoint:** `GET /api/database/{type}/{id}`

**Description:** Fetch details of a specific database entity. This is optional and currently not used by the frontend, but provided for future use.

---

### Request Format

**Method:** `GET`  
**Headers:** `Content-Type: application/json`  
**URL:** `{BASE_URL}/database/{type}/{id}`

#### Path Parameters:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `type` | string | Yes | One of: `"table"`, `"collection"`, or `"media"` |
| `id` | string | Yes | ID or name of the entity |

#### Example Requests:

```
GET /api/database/table/users
GET /api/database/collection/logs
GET /api/database/media/directory_1703123456789
```

---

### Response Format

**Status Code:** `200 OK`  
**Content-Type:** `application/json`

#### Example Response for Table:

```json
{
  "name": "table_users",
  "type": "SQL",
  "columns": [
    {
      "name": "id",
      "type": "INTEGER"
    },
    {
      "name": "name",
      "type": "VARCHAR(255)"
    }
  ],
  "relationships": [
    {
      "target": "table_orders",
      "type": "one-to-many"
    }
  ]
}
```

---

## Error Response Format

All endpoints should return appropriate error responses with consistent formatting.

---

### Error Response Structure:

**Status Code:** `400 Bad Request`, `500 Internal Server Error`, etc.

```json
{
  "success": false,
  "error": "Error message describing what went wrong",
  "code": "ERROR_CODE"
}
```

#### Example Error Responses:

**400 Bad Request (Invalid Data):**
```json
{
  "success": false,
  "error": "Invalid JSON format in request body",
  "code": "INVALID_JSON"
}
```

**500 Internal Server Error:**
```json
{
  "success": false,
  "error": "Failed to process files: Database connection error",
  "code": "DATABASE_ERROR"
}
```

**404 Not Found (for GET /database/{type}/{id}):**
```json
{
  "success": false,
  "error": "Entity not found",
  "code": "NOT_FOUND"
}
```

---

## Complete Data Structure Definitions

### SQL Table Object

```typescript
interface SQLTable {
  name: string;              // Table name (e.g., "table_users")
  type: "SQL";               // Always "SQL"
  columns: Column[];         // Array of column definitions
  relationships: Relationship[];  // Array of relationships
}

interface Column {
  name: string;              // Column name (e.g., "email")
  type: string;              // Data type (e.g., "VARCHAR(255)", "INTEGER", "BOOLEAN")
}

interface Relationship {
  target: string;            // Target table name
  type: string;              // Relationship type (e.g., "one-to-many", "many-to-one", "one-to-one")
}
```

### NoSQL Collection Object

```typescript
interface NoSQLCollection {
  name: string;              // Collection name (e.g., "collection_logs")
  count: number;             // Number of documents in collection
  schema: object;            // Schema definition (can be any object structure)
                            // Example: { "field1": "string", "field2": "number", "nested": { "prop": "string" } }
}
```

### Media Directory Object

```typescript
interface MediaDirectory {
  name: string;              // Directory identifier (e.g., "directory_1703123456789")
  category: string;          // Category name (e.g., "Photos", "Videos", "Media")
  files: FileInfo[];         // Array of file information
}

interface FileInfo {
  name: string;              // File name (e.g., "photo1.jpg")
  size: number;              // File size in bytes
  type: string;              // MIME type (e.g., "image/jpeg", "video/mp4")
}
```

### Complete Database State Object

```typescript
interface DatabaseState {
  tables: SQLTable[];                    // Array of SQL tables (empty if none)
  collections: NoSQLCollection[];        // Array of NoSQL collections (empty if none)
  mediaDirectories: MediaDirectory[];    // Array of media directories (empty if none)
}
```

### Upload Response Object

```typescript
interface UploadResponse {
  success: boolean;                      // Always true for success
  message: string;                       // Success message
  databaseState: DatabaseState;          // Updated database state (required)
}
```

---

## Frontend Behavior After API Calls

### After Upload:

1. Frontend calls `POST /api/upload` with files and optional metadata
2. If response contains `databaseState`, frontend uses it directly
3. If response doesn't contain `databaseState`, frontend calls `GET /api/database/state`
4. Frontend updates visualization with new state
5. If error occurs, frontend displays error message to user

### On Page Load:

1. Frontend calls `GET /api/database/state` on component mount
2. Frontend displays visualization with returned state
3. If error occurs, frontend may use mock data (development only) or show error

---

## Testing Checklist for Backend Developers

- [ ] Upload endpoint (`POST /api/upload`) accepts `multipart/form-data` for file uploads
- [ ] Upload endpoint handles multiple files of different types in one request
- [ ] Upload endpoint supports folder uploads (files with directory structure)
- [ ] All endpoints return `Content-Type: application/json` for JSON responses
- [ ] Upload endpoint response includes `databaseState` field
- [ ] `databaseState` always includes all three arrays: `tables`, `collections`, `mediaDirectories`
- [ ] Empty arrays are returned as `[]` not `null`
- [ ] Error responses follow the error format with `success: false`
- [ ] HTTP status codes are appropriate (200 for success, 4xx for client errors, 5xx for server errors)
- [ ] Backend intelligently categorizes files (images→media, JSON→SQL/NoSQL, etc.)
- [ ] CORS headers are configured to allow frontend origin

---

## Additional Notes

1. **CORS Configuration**: Ensure your backend allows requests from your frontend origin. Add appropriate CORS headers.

2. **File Size Limits**: Consider implementing file size limits for uploads to prevent abuse.

3. **Authentication**: Currently, the frontend doesn't send authentication tokens. If you need authentication, add it to the headers in `src/services/api.js`.

4. **Pagination**: For large datasets, consider implementing pagination in `GET /api/database/state` if needed in the future.

5. **Real-time Updates**: Currently, the frontend polls for updates after uploads. You may want to implement WebSocket support for real-time updates in the future.

---

## Contact & Support

For questions about the frontend implementation or this API specification, refer to:
- Frontend code: `src/services/api.js` - Contains all API call implementations
- Frontend main logic: `src/App.jsx` - Contains data submission and state management

---

**Document Version:** 1.0  
**Last Updated:** 2024

