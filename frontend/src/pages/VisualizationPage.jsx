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
  FileText,
  Play
} from 'lucide-react'
import { getVisualizationData, searchFiles, downloadFile, searchVideos } from '../services/api'

const VisualizationPage = ({ onNavigate }) => {
  const [fileTree, setFileTree] = useState(null)
  const [expandedFolders, setExpandedFolders] = useState(new Set())
  const [searchQuery, setSearchQuery] = useState('')
  const [searchResults, setSearchResults] = useState([])
  const [videoSearchQuery, setVideoSearchQuery] = useState('')
  const [videoSearchResults, setVideoSearchResults] = useState([])
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

  const handleVideoSearch = async (description) => {
    if (!description.trim()) {
      setVideoSearchResults([])
      return
    }

    try {
      setIsLoading(true)
      // TODO: Replace with real API call when backend is ready
      // const results = await searchVideos(description)
      // setVideoSearchResults(results)
      
      // Mock video search results for now
      const mockResults = [
        { 
          path: 'Media/video1.mp4', 
          name: 'video1.mp4', 
          type: 'video',
          description: 'A video showing nature scenes',
          duration: '2:34',
          thumbnail: null
        },
        { 
          path: 'Media/video2.mp4', 
          name: 'video2.mp4', 
          type: 'video',
          description: 'A tutorial video about coding',
          duration: '5:12',
          thumbnail: null
        }
      ]
      setVideoSearchResults(mockResults)
    } catch (err) {
      console.error('Video search failed:', err)
      setError('Video search failed')
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

        {/* Video Search Section */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.4 }}
          className="mt-12 mb-8"
        >
          <div className="text-center mb-6">
            <div className="flex items-center justify-center gap-2 mb-4">
              <Play className="w-8 h-8 text-purple-400" />
              <h2 className="text-3xl md:text-4xl font-bold bg-gradient-to-r from-purple-400 to-pink-400 bg-clip-text text-transparent p-1">
                Video Search
              </h2>
            </div>
            <p className="text-gray-400">
              Describe what you're looking for in a video and find it instantly
            </p>
          </div>

          {/* Two-column layout: Explanation + Search on left, Results on right */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Left Column: Explanation and Search */}
            <motion.div
              initial={{ opacity: 0, x: -20 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: 0.5 }}
              className="glass rounded-xl p-6"
            >
              <div className="mb-6">
                <h3 className="text-xl font-bold mb-4 text-purple-400 flex items-center gap-2">
                  <Video className="w-6 h-6" />
                  How It Works
                </h3>
                <div className="space-y-3 text-gray-300 text-sm">
                  <p>
                    Our AI-powered video search understands natural language descriptions. Simply describe what you're looking for in the video:
                  </p>
                  <ul className="list-disc list-inside space-y-2 ml-2 text-gray-400">
                    <li>Describe the content or scenes you want to find</li>
                    <li>Mention objects, actions, or concepts in the video</li>
                    <li>Specify any particular characteristics or themes</li>
                    <li>Our system will analyze and match your description</li>
                  </ul>
                  <p className="text-gray-500 italic">
                    Example: "A video showing a sunset over mountains with birds flying"
                  </p>
                </div>
              </div>
              
              <div className="mt-6">
                <label className="block text-sm font-semibold text-gray-400 mb-2">
                  Describe the video you're looking for:
                </label>
                <div className="flex flex-col gap-3">
                  <textarea
                    value={videoSearchQuery}
                    onChange={(e) => {
                      setVideoSearchQuery(e.target.value)
                    }}
                    placeholder="e.g., A video showing nature scenes with waterfalls..."
                    rows="4"
                    className="w-full bg-black/50 border border-gray-700 rounded-lg p-3 text-white focus:outline-none focus:border-purple-400 focus:ring-2 focus:ring-purple-400/50 resize-none"
                  />
                  <motion.button
                    onClick={() => handleVideoSearch(videoSearchQuery)}
                    whileHover={{ scale: 1.02 }}
                    whileTap={{ scale: 0.98 }}
                    disabled={!videoSearchQuery.trim() || isLoading}
                    className="px-6 py-3 bg-gradient-to-r from-purple-600 to-pink-600 text-white font-semibold rounded-lg hover:shadow-lg hover:shadow-purple-500/50 transition-all duration-300 flex items-center justify-center gap-2 disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    <Search className="w-5 h-5" />
                    {isLoading ? 'Searching...' : 'Search Videos'}
                  </motion.button>
                </div>
              </div>
            </motion.div>

            {/* Right Column: Results */}
            <motion.div
              initial={{ opacity: 0, x: 20 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: 0.6 }}
              className="glass rounded-xl p-6"
            >
              <h3 className="text-xl font-bold mb-4 text-purple-400 flex items-center gap-2">
                <Search className="w-6 h-6" />
                Search Results
              </h3>
              
              {videoSearchQuery ? (
                videoSearchResults.length > 0 ? (
                  <div className="space-y-4">
                    {videoSearchResults.map((result, index) => (
                      <motion.div
                        key={index}
                        initial={{ opacity: 0, y: 10 }}
                        animate={{ opacity: 1, y: 0 }}
                        transition={{ delay: index * 0.1 }}
                        className="p-4 bg-black/40 rounded-lg hover:bg-black/60 transition-colors border border-gray-700 hover:border-purple-500/50"
                      >
                        <div className="flex items-start gap-4">
                          <div className="flex-shrink-0 w-24 h-16 bg-purple-900/30 rounded flex items-center justify-center">
                            {result.thumbnail ? (
                              <img 
                                src={result.thumbnail} 
                                alt={result.name}
                                className="w-full h-full object-cover rounded"
                              />
                            ) : (
                              <Video className="w-8 h-8 text-purple-400" />
                            )}
                          </div>
                          <div className="flex-1 min-w-0">
                            <div className="flex items-start justify-between gap-2 mb-2">
                              <h4 className="text-white font-semibold truncate">{result.name}</h4>
                              {result.duration && (
                                <span className="text-xs text-gray-400 bg-black/50 px-2 py-1 rounded flex-shrink-0">
                                  {result.duration}
                                </span>
                              )}
                            </div>
                            <p className="text-sm text-gray-400 mb-2 line-clamp-2">
                              {result.description || 'No description available'}
                            </p>
                            <p className="text-xs text-gray-500 truncate">{result.path}</p>
                            <div className="flex items-center gap-2 mt-3">
                              <motion.button
                                onClick={() => handleDownload(result.path)}
                                whileHover={{ scale: 1.05 }}
                                whileTap={{ scale: 0.95 }}
                                className="px-3 py-1.5 bg-purple-600/20 text-purple-400 text-xs font-medium rounded hover:bg-purple-600/30 transition-colors flex items-center gap-1"
                              >
                                <Download className="w-3 h-3" />
                                Download
                              </motion.button>
                            </div>
                          </div>
                        </div>
                      </motion.div>
                    ))}
                  </div>
                ) : (
                  <div className="text-center py-12 text-gray-400">
                    <Video className="w-16 h-16 mx-auto mb-4 opacity-50" />
                    <p>No videos found matching your description.</p>
                    <p className="text-sm text-gray-500 mt-2">Try a different description or check your search terms.</p>
                  </div>
                )
              ) : (
                <div className="text-center py-12 text-gray-500">
                  <Search className="w-16 h-16 mx-auto mb-4 opacity-30" />
                  <p>Enter a video description to see results here</p>
                </div>
              )}
            </motion.div>
          </div>
        </motion.div>

        {/* Note about endpoints */}
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 0.7 }}
          className="glass rounded-xl p-4 mt-6 bg-blue-900/20 border border-blue-500/30"
        >
          <p className="text-sm text-blue-300">
            📁 <strong>Note:</strong> Currently showing mock data. Backend endpoints are ready:
            <br />
            <code className="bg-black/50 px-2 py-1 rounded">GET /api/visualization</code> - Get file tree structure
            <br />
            <code className="bg-black/50 px-2 py-1 rounded">GET /api/search?q=query</code> - Search files
            <br />
            <code className="bg-black/50 px-2 py-1 rounded">GET /api/search/videos?description=text</code> - Search videos by description
            <br />
            <code className="bg-black/50 px-2 py-1 rounded">GET /api/download?path=filepath</code> - Download file
          </p>
        </motion.div>
      </motion.div>
    </div>
  )
}

export default VisualizationPage

