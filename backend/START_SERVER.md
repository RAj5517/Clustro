# Starting the Clustro Backend Server

## Quick Start

1. **Install dependencies:**
   ```bash
   cd Backend
   pip install -r requirements.txt
   ```

2. **Set up database:**
   ```bash
   python Schema_generator/database_setup.py
   ```

3. **Start the Flask server:**
   ```bash
   python app.py
   ```

The server will start on `http://localhost:8000`

## Verify Everything is Working

### 1. Check Server is Running
Open: http://localhost:8000/api/health

Should return:
```json
{
  "status": "healthy",
  "message": "Clustro API is running"
}
```

### 2. View Database Tables
```bash
python view_tables.py
```

This will show all tables created from SQL files you upload.

## Testing the Full Flow

1. **Start the backend server:**
   ```bash
   cd Backend
   python app.py
   ```

2. **Start the frontend** (in another terminal):
   ```bash
   cd frontend
   npm run dev
   ```

3. **Upload SQL files** through the frontend UI:
   - Go to Upload page
   - Select JSON, CSV, or XML files
   - Click "Submit Data"

4. **View created tables:**
   ```bash
   python view_tables.py
   ```

   Or check the API:
   ```bash
   curl http://localhost:8000/api/database/tables
   ```

## API Endpoints

- `POST /api/upload` - Upload files/folders
- `GET /api/database/state` - Get database state
- `GET /api/database/tables` - List all tables
- `GET /api/database/tables/<name>` - Get table details
- `GET /api/health` - Health check

## Troubleshooting

### Database Connection Issues
1. Check PostgreSQL is running
2. Verify `.env` file or environment variables
3. Test connection: `python Schema_generator/quick_test.py`

### Port Already in Use
Change port in `app.py`:
```python
app.run(host='0.0.0.0', port=8000, debug=True)  # Change 8000 to another port
```

### Frontend Can't Connect
Make sure frontend is pointing to the correct API URL. Check `frontend/src/services/api.js`:
```javascript
const BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000/api'
```

