import { useState, useEffect } from 'react'
import { motion } from 'framer-motion'
import { 
  Folder, 
  File, 
  Search, 
  Download, 
  ChevronRight, 
  ChevronDown,
  Sparkles,
  Database,
  Image,
  Video,
  FileText
} from 'lucide-react'
import { getVisualizationData, searchFiles, downloadFile } from '../services/api'

const VisualizationPage = ({ onNavigate }) => {
  const [fileTree, setFileTree] = useState(null)
  const [expandedFolders, setExpandedFolders] = useState(new Set())
  const [searchQuery, setSearchQuery] = useState('')
  const [searchResults, setSearchResults] = useState([])
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState(null)

  // Mock file tree structure for now
  const mockFileTree = {
    name: 'Root',
    type: 'folder',
    children: [
      {
        name: 'Media',
        type: 'folder',
        children: [
          { name: 'photo1.jpg', type: 'file', size: '2.4 MB', mimeType: 'image/jpeg' },
          { name: 'photo2.png', type: 'file', size: '1.8 MB', mimeType: 'image/png' },
          { name: 'video1.mp4', type: 'file', size: '45 MB', mimeType: 'video/mp4' }
        ]
      },
      {
        name: 'Documents',
        type: 'folder',
        children: [
          { name: 'data.json', type: 'file', size: '120 KB', mimeType: 'application/json' },
          { name: 'report.pdf', type: 'file', size: '3.2 MB', mimeType: 'application/pdf' },
          { name: 'notes.txt', type: 'file', size: '5 KB', mimeType: 'text/plain' }
        ]
      },
      {
        name: 'Database',
        type: 'folder',
        children: [
          {
            name: 'SQL Tables',
            type: 'folder',
            children: [
              { name: 'users.sql', type: 'file', size: '15 KB', mimeType: 'application/sql' },
              { name: 'products.sql', type: 'file', size: '22 KB', mimeType: 'application/sql' }
            ]
          },
          {
            name: 'NoSQL Collections',
            type: 'folder',
            children: [
              { name: 'logs.json', type: 'file', size: '8 KB', mimeType: 'application/json' },
              { name: 'sessions.json', type: 'file', size: '12 KB', mimeType: 'application/json' }
            ]
          }
        ]
      }
    ]
  }

  useEffect(() => {
    // TODO: Replace with real API call when backend is ready
    // fetchVisualizationData()
    setFileTree(mockFileTree)
    // Expand root by default
    setExpandedFolders(new Set(['Root']))
  }, [])

  const fetchVisualizationData = async () => {
    try {
      setIsLoading(true)
      const data = await getVisualizationData()
      setFileTree(data)
      setError(null)
    } catch (err) {
      console.error('Failed to fetch visualization data:', err)
      setError('Failed to load file structure. Showing mock data.')
      setFileTree(mockFileTree)
    } finally {
      setIsLoading(false)
    }
  }

  const handleSearch = async (query) => {
    if (!query.trim()) {
      setSearchResults([])
      return
    }

    try {
      setIsLoading(true)
      // TODO: Replace with real API call when backend is ready
      // const results = await searchFiles(query)
      // setSearchResults(results)
      
      // Mock search results for now
      const mockResults = [
        { path: 'Media/photo1.jpg', name: 'photo1.jpg', type: 'file' },
        { path: 'Documents/data.json', name: 'data.json', type: 'file' }
      ]
      setSearchResults(mockResults)
    } catch (err) {
      console.error('Search failed:', err)
      setError('Search failed')
    } finally {
      setIsLoading(false)
    }
  }

  const handleDownload = async (filePath) => {
    try {
      // TODO: Replace with real API call when backend is ready
      // await downloadFile(filePath)
      console.log('Download requested for:', filePath)
      alert(`Download functionality will be implemented. File: ${filePath}`)
    } catch (err) {
      console.error('Download failed:', err)
      setError('Download failed')
    }
  }

  const toggleFolder = (folderPath) => {
    const newExpanded = new Set(expandedFolders)
    if (newExpanded.has(folderPath)) {
      newExpanded.delete(folderPath)
    } else {
      newExpanded.add(folderPath)
    }
    setExpandedFolders(newExpanded)
  }

  const renderFileTree = (node, path = '') => {
    const currentPath = path ? `${path}/${node.name}` : node.name
    const isExpanded = expandedFolders.has(currentPath)
    const isFolder = node.type === 'folder'

    return (
      <div key={currentPath} className="select-none">
        <div
          className="flex items-center gap-2 p-2 hover:bg-gray-800/50 rounded cursor-pointer group"
          onClick={() => isFolder && toggleFolder(currentPath)}
        >
          {isFolder ? (
            <>
              {isExpanded ? (
                <ChevronDown className="w-4 h-4 text-gray-400" />
              ) : (
                <ChevronRight className="w-4 h-4 text-gray-400" />
              )}
              <Folder className="w-5 h-5 text-yellow-400" />
            </>
          ) : (
            <>
              <div className="w-4"></div>
              {node.mimeType?.startsWith('image/') ? (
                <Image className="w-5 h-5 text-blue-400" />
              ) : node.mimeType?.startsWith('video/') ? (
                <Video className="w-5 h-5 text-red-400" />
              ) : (
                <FileText className="w-5 h-5 text-gray-400" />
              )}
            </>
          )}
          <span className="text-gray-300 flex-1">{node.name}</span>
          {!isFolder && node.size && (
            <span className="text-xs text-gray-500">{node.size}</span>
          )}
          {!isFolder && (
            <button
              onClick={(e) => {
                e.stopPropagation()
                handleDownload(currentPath)
              }}
              className="opacity-0 group-hover:opacity-100 transition-opacity p-1 hover:bg-purple-500/20 rounded"
            >
              <Download className="w-4 h-4 text-purple-400" />
            </button>
          )}
        </div>
        {isFolder && isExpanded && node.children && (
          <div className="ml-6 border-l border-gray-700 pl-2">
            {node.children.map((child) => renderFileTree(child, currentPath))}
          </div>
        )}
      </div>
    )
  }

  return (
    <div className="min-h-screen p-4 md:p-8">
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        className="max-w-7xl mx-auto"
      >
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          className="text-center mb-12"
        >
          <div className="flex items-center justify-center gap-2 mb-4">
            <Sparkles className="w-8 h-8 text-purple-400" />
            <h2 className="text-3xl md:text-4xl font-bold bg-gradient-to-r from-purple-400 to-pink-400 bg-clip-text text-transparent p-1">
              Data Visualization
            </h2>
          </div>
          <p className="text-gray-400">
            Browse and download your stored files in a tree structure
          </p>
        </motion.div>

        {/* Search Bar */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          className="glass rounded-xl p-4 mb-6"
        >
          <div className="flex items-center gap-3">
            <Search className="w-5 h-5 text-gray-400" />
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => {
                setSearchQuery(e.target.value)
                handleSearch(e.target.value)
              }}
              placeholder="Search files and folders..."
              className="flex-1 bg-black/50 border border-gray-700 rounded-lg p-3 text-white focus:outline-none focus:border-purple-400 focus:ring-2 focus:ring-purple-400/50"
            />
          </div>
        </motion.div>

        {error && (
          <motion.div
            initial={{ opacity: 0, y: -20 }}
            animate={{ opacity: 1, y: 0 }}
            className="mb-6 p-4 bg-red-900/20 border border-red-500/50 rounded-lg text-red-300 text-sm"
          >
            ⚠️ {error}
          </motion.div>
        )}

        {/* Search Results or File Tree */}
        {searchQuery ? (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            className="glass rounded-xl p-6"
          >
            <h3 className="text-lg font-semibold mb-4 text-purple-400">
              Search Results for "{searchQuery}"
            </h3>
            {searchResults.length > 0 ? (
              <div className="space-y-2">
                {searchResults.map((result, index) => (
                  <div
                    key={index}
                    className="flex items-center justify-between p-3 hover:bg-gray-800/50 rounded"
                  >
                    <div className="flex items-center gap-3">
                      <FileText className="w-5 h-5 text-gray-400" />
                      <div>
                        <p className="text-white">{result.name}</p>
                        <p className="text-xs text-gray-500">{result.path}</p>
                      </div>
                    </div>
                    <button
                      onClick={() => handleDownload(result.path)}
                      className="p-2 hover:bg-purple-500/20 rounded"
                    >
                      <Download className="w-4 h-4 text-purple-400" />
                    </button>
                  </div>
                ))}
              </div>
            ) : (
              <p className="text-gray-400">No results found</p>
            )}
          </motion.div>
        ) : (
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            className="glass rounded-xl p-6"
          >
            <div className="flex items-center gap-2 mb-4">
              <Database className="w-5 h-5 text-purple-400" />
              <h3 className="text-lg font-semibold text-purple-400">File Structure</h3>
            </div>
            {fileTree ? (
              <div className="space-y-1">
                {renderFileTree(fileTree)}
              </div>
            ) : (
              <p className="text-gray-400">Loading file structure...</p>
            )}
          </motion.div>
        )}

        {/* Note about endpoints */}
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 0.3 }}
          className="glass rounded-xl p-4 mt-6 bg-blue-900/20 border border-blue-500/30"
        >
          <p className="text-sm text-blue-300">
            📁 <strong>Note:</strong> Currently showing mock data. Backend endpoints are ready:
            <br />
            <code className="bg-black/50 px-2 py-1 rounded">GET /api/visualization</code> - Get file tree structure
            <br />
            <code className="bg-black/50 px-2 py-1 rounded">GET /api/search?q=query</code> - Search files
            <br />
            <code className="bg-black/50 px-2 py-1 rounded">GET /api/download?path=filepath</code> - Download file
          </p>
        </motion.div>
      </motion.div>
    </div>
  )
}

export default VisualizationPage

