# API Quick Reference Guide

A quick reference for backend developers implementing the AURAverse API.

---

## Base URL

**Default:** `http://localhost:8000/api`

---

## Endpoints Summary

| Endpoint | Method | Content-Type | Description |
|----------|--------|--------------|-------------|
| `/api/upload` | POST | `multipart/form-data` | Upload files or folder (any file type) |
| `/api/database/state` | GET | `application/json` | Get current database state |
| `/api/database/{type}/{id}` | GET | `application/json` | Get specific entity (optional) |

---

## Request/Response Formats

### 1. POST /api/upload

**Request:**
```
multipart/form-data:
  - files: File[] (required) - Array of files (any type)
  - metadata: string (optional)
```

**Response:**
```json
{
  "success": true,
  "message": "Files uploaded and processed successfully",
  "databaseState": {
    "tables": [{
      "name": "table_users",
      "type": "SQL",
      "columns": [{
        "name": "name",
        "type": "VARCHAR(255)"
      }],
      "relationships": []
    }],
    "collections": [{
      "name": "collection_logs",
      "count": 1,
      "schema": { /* schema object */ }
    }],
    "mediaDirectories": [{
      "name": "directory_123",
      "category": "Photos",
      "files": [{
        "name": "photo.jpg",
        "size": 1024000,
        "type": "image/jpeg"
      }]
    }]
  }
}
```

**Notes:**
- Backend analyzes all files and categorizes intelligently
- Images/Videos → `mediaDirectories`
- JSON files → `tables` or `collections` (based on structure)
- Other files → `collections` with metadata

---

### 2. GET /api/database/state

**Request:** None (no query parameters)

**Response:**
```json
{
  "tables": [ /* SQL table objects */ ],
  "collections": [ /* NoSQL collection objects */ ],
  "mediaDirectories": [ /* Media directory objects */ ]
}
```

**Important:** All three arrays are required even if empty.

---

## Data Structures

### SQL Table
```json
{
  "name": "table_users",
  "type": "SQL",
  "columns": [
    { "name": "id", "type": "INTEGER" },
    { "name": "email", "type": "VARCHAR(255)" }
  ],
  "relationships": [
    { "target": "table_orders", "type": "one-to-many" }
  ]
}
```

### NoSQL Collection
```json
{
  "name": "collection_logs",
  "count": 150,
  "schema": {
    "timestamp": "string",
    "level": "string",
    "message": "string"
  }
}
```

### Media Directory
```json
{
  "name": "directory_123",
  "category": "Photos",
  "files": [
    {
      "name": "photo.jpg",
      "size": 1024000,
      "type": "image/jpeg"
    }
  ]
}
```

---

## Error Response

**Status Code:** 4xx or 5xx

```json
{
  "success": false,
  "error": "Error message",
  "code": "ERROR_CODE"
}
```

---

## Key Points

1. ✅ **Single upload endpoint** - All files go to `POST /api/upload`
2. ✅ **Always return `databaseState`** in upload responses
3. ✅ **All three arrays required** in GET /database/state (tables, collections, mediaDirectories)
4. ✅ **Empty arrays = `[]`** not `null`
5. ✅ **Backend analyzes files** - Images→Media, JSON→SQL/NoSQL, etc.
6. ✅ **Folder uploads supported** - Files maintain directory structure
7. ✅ **Use proper HTTP status codes** (200, 400, 500, etc.)
8. ✅ **Set Content-Type headers** correctly

---

## Frontend Behavior

- **After Upload:** Frontend calls `POST /api/upload` with files. Checks for `databaseState` in response. If missing, calls `GET /api/database/state`
- **On Page Load:** Frontend calls `GET /api/database/state` to initialize visualization
- **On Error:** Frontend displays error message to user
- **Upload Types:** Users can upload individual files or entire folders

---

For detailed documentation, see: **BACKEND_API_SPECIFICATION.md**

