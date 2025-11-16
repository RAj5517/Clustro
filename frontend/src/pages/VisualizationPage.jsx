import { useState, useEffect, useRef } from 'react'
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
  Play,
  RefreshCw
} from 'lucide-react'
import { getVisualizationData, downloadFile, getDatabaseState, semanticSearch } from '../services/api'

const VisualizationPage = ({ onNavigate }) => {
  const [fileTree, setFileTree] = useState(null)
  const [expandedFolders, setExpandedFolders] = useState(new Set())
  const [searchQuery, setSearchQuery] = useState('')
  const [searchResults, setSearchResults] = useState([])
  const [videoSearchQuery, setVideoSearchQuery] = useState('')
  const [videoSearchResults, setVideoSearchResults] = useState([])
  const [fileTreeLoading, setFileTreeLoading] = useState(false)
  const [semanticLoading, setSemanticLoading] = useState(false)
  const [error, setError] = useState(null)
  const [databaseState, setDatabaseState] = useState(null)
  const [schemaLoading, setSchemaLoading] = useState(false)
  const [schemaError, setSchemaError] = useState(null)
  const [semanticFilterResults, setSemanticFilterResults] = useState([])
  const [semanticFilterLoading, setSemanticFilterLoading] = useState(false)
  const [semanticFilterError, setSemanticFilterError] = useState(null)
  const [semanticFilterSource, setSemanticFilterSource] = useState('semantic')
  const [videoSemanticSource, setVideoSemanticSource] = useState('semantic')
  const semanticRequestRef = useRef(0)

  useEffect(() => {
    fetchVisualizationData()
    fetchDatabaseSchema()
  }, [])

  useEffect(() => {
    if (fileTree?.name) {
      setExpandedFolders(new Set([fileTree.name]))
    }
  }, [fileTree])

  const VIDEO_EXTENSIONS = ['.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.mkv', '.m4v', '.3gp']

  const isVideoNode = (node) => {
    if (!node || node.type !== 'file') return false
    const mimeType = node.mimeType || ''
    if (mimeType.startsWith('video/')) return true
    const name = (node.name || '').toLowerCase()
    return VIDEO_EXTENSIONS.some((ext) => name.endsWith(ext))
  }

  const fileMatchesQuery = (node, currentPath, normalizedQuery) => {
    const name = (node?.name || '').toLowerCase()
    const pathValue = currentPath.toLowerCase()
    return name.includes(normalizedQuery) || pathValue.includes(normalizedQuery)
  }

  const collectMatches = (query, predicate = () => true) => {
    const normalizedQuery = query.trim().toLowerCase()
    if (!normalizedQuery || !fileTree) {
      return []
    }

    const matches = []

    const walk = (node, path = '') => {
      if (!node?.name) return
      const currentPath = path ? `${path}/${node.name}` : node.name
      const isFolder = node.type === 'folder'
      const textMatch = fileMatchesQuery(node, currentPath, normalizedQuery)
      if (textMatch && predicate(node)) {
        matches.push({
          name: node.name,
          path: currentPath,
          type: node.type,
          mimeType: node.mimeType,
          size: node.size,
          collection: node.collection,
          isFolder,
          downloadPath: node.storagePath || node.storage_uri || node.original_name || (node.type === 'file' ? currentPath : null)
        })
      }

      if (isFolder && Array.isArray(node.children)) {
        node.children.forEach((child) => walk(child, currentPath))
      }
    }

    walk(fileTree)
    return matches
  }

  const normalizeSemanticResults = (items = []) => {
    return items.map((item) => {
      const metadata = item.metadata || {}
      const rawPath = metadata.path || metadata.storage_uri || metadata.original_name || item.path || ''
      const name = metadata.original_name || rawPath?.split('/').pop() || metadata.file_id || item.id
      return {
        id: item.id,
        name,
        path: rawPath,
        modality: item.modality || metadata.modality || metadata.collection || 'unknown',
        similarity: item.similarity,
        description: item.text || metadata.summary || '',
        metadata,
        isChunk: (metadata.type || '').toLowerCase() === 'chunk',
        downloadPath: rawPath || metadata.storage_uri || null,
      }
    })
  }

  const fetchDatabaseSchema = async () => {
    try {
      setSchemaLoading(true)
      setSchemaError(null)
      const state = await getDatabaseState()
      setDatabaseState(state)
    } catch (err) {
      console.error('Failed to fetch database schema:', err)
      setSchemaError('Failed to load database schema')
      setDatabaseState(null)
    } finally {
      setSchemaLoading(false)
    }
  }

  const fetchVisualizationData = async () => {
    try {
      setFileTreeLoading(true)
      const data = await getVisualizationData()
      setFileTree(data)
      setError(null)
    } catch (err) {
      console.error('Failed to fetch visualization data:', err)
      setError('Failed to load file structure from the database.')
      setFileTree(null)
    } finally {
      setFileTreeLoading(false)
    }
  }

  const handleSearch = (query) => {
    if (!query.trim()) {
      setSearchResults([])
      setSemanticFilterResults([])
      setSemanticFilterLoading(false)
      setSemanticFilterError(null)
      setSemanticFilterSource('semantic')
      return
    }

    if (!fileTree) {
      setError('File structure is not available yet. Please refresh the visualization.')
      setSemanticFilterSource('semantic')
      return
    }

    setError(null)
    const results = collectMatches(query)
    setSearchResults(results)

    if (query.trim().length < 3) {
      setSemanticFilterResults([])
      setSemanticFilterLoading(false)
      setSemanticFilterError(null)
      return
    }

    const requestToken = Date.now()
    semanticRequestRef.current = requestToken
    setSemanticFilterLoading(true)
    setSemanticFilterError(null)

    semanticSearch(query, 8)
      .then((response) => {
        if (semanticRequestRef.current !== requestToken) return
        if (!response?.success) {
          throw new Error(response?.error || 'Semantic search failed')
        }
        setSemanticFilterResults(normalizeSemanticResults(response.results || []))
        setSemanticFilterSource(response.source || 'semantic')
      })
      .catch((err) => {
        console.error('Semantic filter search failed:', err)
        if (semanticRequestRef.current === requestToken) {
          setSemanticFilterResults([])
          setSemanticFilterError('Semantic search unavailable for this query.')
          setSemanticFilterSource('semantic')
        }
      })
      .finally(() => {
        if (semanticRequestRef.current === requestToken) {
          setSemanticFilterLoading(false)
        }
      })
  }

  const handleVideoSearch = async (description) => {
    if (!description.trim()) {
      setVideoSearchResults([])
      return
    }

    try {
      setSemanticLoading(true)
      setError(null)
      const response = await semanticSearch(description, 12)
      if (!response?.success) {
        throw new Error(response?.error || 'Semantic search failed')
      }

      const normalized = normalizeSemanticResults(response.results || [])
      setVideoSearchResults(normalized)
      setVideoSemanticSource(response.source || 'semantic')
    } catch (err) {
      console.error('Semantic search failed:', err)
      setError('Semantic search failed. Please try again.')
      setVideoSearchResults([])
      setVideoSemanticSource('semantic')
    } finally {
      setSemanticLoading(false)
    }
  }

  const handleDownload = async (filePath) => {
    if (!filePath) {
      setError('No file path available for download.')
      return
    }
    try {
      await downloadFile(filePath)
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
    if (!node) return null

    const currentPath = path ? `${path}/${node.name}` : node.name
    const isExpanded = expandedFolders.has(currentPath)
    const isFolder = node.type === 'folder'
    const children = Array.isArray(node.children) ? node.children : []

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
                handleDownload(node.storagePath || currentPath)
              }}
              className="opacity-0 group-hover:opacity-100 transition-opacity p-1 hover:bg-purple-500/20 rounded"
            >
              <Download className="w-4 h-4 text-purple-400" />
            </button>
          )}
        </div>
        {isFolder && isExpanded && children.length > 0 && (
          <div className="ml-6 border-l border-gray-700 pl-2">
            {children.map((child) => renderFileTree(child, currentPath))}
          </div>
        )}
      </div>
    )
  }

  const semanticMatchesLabel = semanticFilterSource === 'metadata' ? 'Metadata Matches' : 'Semantic Matches'
  const videoMatchesLabel = videoSemanticSource === 'metadata' ? 'Metadata Matches' : 'Semantic Matches'

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

        {/* SQL Schema Section */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          className="glass rounded-xl p-6 mb-6 border border-purple-500/30"
        >
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-2">
              <Database className="w-6 h-6 text-purple-400" />
              <h3 className="text-xl font-bold text-purple-400">SQL Schema</h3>
            </div>
            <button
              onClick={fetchDatabaseSchema}
              disabled={schemaLoading}
              className="px-3 py-1.5 bg-purple-600/20 text-purple-400 text-sm font-medium rounded hover:bg-purple-600/30 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {schemaLoading ? 'Refreshing...' : 'Refresh'}
            </button>
          </div>

          {schemaLoading && !databaseState ? (
            <div className="text-center py-8 text-gray-400">
              <Database className="w-12 h-12 mx-auto mb-2 opacity-50 animate-pulse" />
              <p>Loading database schema...</p>
            </div>
          ) : schemaError ? (
            <div className="p-4 bg-red-900/20 border border-red-500/50 rounded-lg text-red-300 text-sm">
              ⚠️ {schemaError}
            </div>
          ) : databaseState?.tables && databaseState.tables.length > 0 ? (
            <div className="space-y-4">
              {databaseState.tables.map((table, index) => (
                <motion.div
                  key={table.name}
                  initial={{ opacity: 0, x: -20 }}
                  animate={{ opacity: 1, x: 0 }}
                  transition={{ delay: index * 0.1 }}
                  className="bg-black/40 rounded-lg p-4 border border-gray-700 hover:border-purple-500/50 transition-colors"
                >
                  <div className="flex items-center gap-2 mb-3">
                    <Database className="w-5 h-5 text-purple-400" />
                    <h4 className="text-lg font-semibold text-white">{table.name}</h4>
                    <span className="text-xs text-gray-400 bg-gray-700 px-2 py-1 rounded">
                      {table.columns?.length || 0} columns
                    </span>
                  </div>
                  
                  {table.columns && table.columns.length > 0 ? (
                    <div className="overflow-x-auto">
                      <table className="w-full text-sm">
                        <thead>
                          <tr className="border-b border-gray-700">
                            <th className="text-left py-2 px-3 text-purple-400 font-semibold">Column Name</th>
                            <th className="text-left py-2 px-3 text-purple-400 font-semibold">Type</th>
                            <th className="text-center py-2 px-3 text-purple-400 font-semibold">Nullable</th>
                          </tr>
                        </thead>
                        <tbody>
                          {table.columns.map((column, colIndex) => (
                            <tr 
                              key={colIndex} 
                              className="border-b border-gray-800/50 hover:bg-gray-800/30 transition-colors"
                            >
                              <td className="py-2 px-3 text-gray-300 font-mono">{column.name}</td>
                              <td className="py-2 px-3 text-gray-400 font-mono text-xs">{column.type}</td>
                              <td className="py-2 px-3 text-center">
                                <span className={`px-2 py-1 rounded text-xs ${
                                  column.nullable 
                                    ? 'bg-yellow-900/30 text-yellow-300' 
                                    : 'bg-red-900/30 text-red-300'
                                }`}>
                                  {column.nullable ? 'NULL' : 'NOT NULL'}
                                </span>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <p className="text-gray-400 text-sm">No columns found</p>
                  )}
                </motion.div>
              ))}
            </div>
          ) : (
            <div className="text-center py-8 text-gray-400">
              <Database className="w-12 h-12 mx-auto mb-2 opacity-50" />
              <p>No SQL tables found</p>
              <p className="text-sm text-gray-500 mt-2">Upload SQL-capable files to create tables</p>
            </div>
          )}
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
                    className="flex items-center justify-between p-3 hover:bg-gray-800/50 rounded border border-gray-800/40"
                  >
                    <div className="flex items-center gap-3">
                      {result.isFolder ? (
                        <Folder className="w-5 h-5 text-yellow-400" />
                      ) : result.mimeType?.startsWith('video/') ? (
                        <Video className="w-5 h-5 text-red-400" />
                      ) : result.mimeType?.startsWith('image/') ? (
                        <Image className="w-5 h-5 text-blue-400" />
                      ) : (
                        <FileText className="w-5 h-5 text-gray-400" />
                      )}
                      <div className="min-w-0">
                        <p className="text-white font-medium truncate">{result.name}</p>
                        <p className="text-xs text-gray-500 truncate">{result.path}</p>
                        <div className="flex items-center gap-2 mt-1 text-[11px] text-gray-400">
                          <span className="uppercase tracking-wide">
                            {result.isFolder ? 'Folder' : 'File'}
                          </span>
                          {!result.isFolder && result.size && (
                            <span className="text-gray-500">{result.size}</span>
                          )}
                        </div>
                      </div>
                    </div>
                    {!result.isFolder && (
                      <button
                        onClick={() => handleDownload(result.downloadPath || result.path)}
                        className="p-2 hover:bg-purple-500/20 rounded"
                      >
                        <Download className="w-4 h-4 text-purple-400" />
                      </button>
                    )}
                  </div>
                ))}
              </div>
            ) : (
              <p className="text-gray-400">No results found</p>
            )}

            {searchQuery.trim().length >= 3 && (
              <div className="mt-6 border-t border-gray-800/60 pt-4">
                <div className="flex items-center gap-2 mb-3">
                  <Sparkles className="w-4 h-4 text-purple-300" />
                  <h4 className="text-sm font-semibold text-purple-300">{semanticMatchesLabel}</h4>
                  {semanticFilterSource === 'metadata' && (
                    <span className="text-[10px] uppercase tracking-wide text-gray-500">fallback</span>
                  )}
                </div>
                {semanticFilterLoading ? (
                  <div className="text-gray-500 text-sm">Analyzing embeddings...</div>
                ) : semanticFilterError ? (
                  <p className="text-xs text-red-400">{semanticFilterError}</p>
                ) : semanticFilterResults.length > 0 ? (
                  <div className="space-y-2">
                    {semanticFilterResults.map((result) => (
                      <div
                        key={result.id}
                        className="p-3 rounded border border-purple-500/20 bg-black/30"
                      >
                        <div className="flex items-center justify-between gap-2">
                          <div>
                            <p className="text-white text-sm font-semibold truncate">{result.name}</p>
                            <p className="text-[11px] text-gray-500 truncate">{result.path || result.metadata?.file_id}</p>
                          </div>
                          {typeof result.similarity === 'number' && (
                            <span className="text-[11px] text-purple-300 bg-purple-600/20 px-2 py-1 rounded">
                              {(result.similarity * 100).toFixed(1)}% match
                            </span>
                          )}
                        </div>
                        {result.description && (
                          <p className="text-xs text-gray-400 mt-2 line-clamp-2">{result.description}</p>
                        )}
                        <div className="flex items-center gap-3 mt-2 text-[11px] text-gray-500">
                          <span className="uppercase tracking-wide">{result.modality}</span>
                          {result.metadata?.collection && <span>{result.metadata.collection}</span>}
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <p className="text-xs text-gray-500">No semantic matches found.</p>
                )}
              </div>
            )}
          </motion.div>
        ) : (
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            className="glass rounded-xl p-6"
          >
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center gap-2">
                <Database className="w-5 h-5 text-purple-400" />
                <h3 className="text-lg font-semibold text-purple-400">File Structure</h3>
              </div>
              <button
                onClick={fetchVisualizationData}
                disabled={fileTreeLoading}
                className="flex items-center gap-1 px-3 py-1.5 text-sm rounded border border-purple-500/40 text-purple-300 hover:bg-purple-500/10 transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
              >
                <RefreshCw className={`w-4 h-4 ${fileTreeLoading ? 'animate-spin' : ''}`} />
                {fileTreeLoading ? 'Refreshing' : 'Refresh'}
              </button>
            </div>
            {fileTreeLoading ? (
              <div className="text-center py-12 text-gray-400">
                <Database className="w-10 h-10 mx-auto mb-2 opacity-50 animate-pulse" />
                <p>Loading file structure from the database...</p>
              </div>
            ) : fileTree ? (
              <div className="space-y-1">
                {renderFileTree(fileTree)}
              </div>
            ) : (
              <div className="text-center py-10 text-gray-500">
                <p>No files have been ingested yet.</p>
                <p className="text-sm text-gray-600 mt-2">Upload data to populate the tree.</p>
              </div>
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
                    disabled={!videoSearchQuery.trim() || semanticLoading}
                    className="px-6 py-3 bg-gradient-to-r from-purple-600 to-pink-600 text-white font-semibold rounded-lg hover:shadow-lg hover:shadow-purple-500/50 transition-all duration-300 flex items-center justify-center gap-2 disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    <Search className={`w-5 h-5 ${semanticLoading ? 'animate-spin' : ''}`} />
                    {semanticLoading ? 'Searching...' : 'Semantic Search'}
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
                {videoMatchesLabel}
                {videoSemanticSource === 'metadata' && (
                  <span className="text-xs text-gray-500 uppercase tracking-wide">fallback</span>
                )}
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
                          <div className="flex-shrink-0 w-14 h-14 bg-purple-900/30 rounded-full flex items-center justify-center">
                            <Video className="w-7 h-7 text-purple-300" />
                          </div>
                          <div className="flex-1 min-w-0">
                            <div className="flex items-center justify-between gap-2 mb-2">
                              <h4 className="text-white font-semibold truncate">{result.name}</h4>
                              <span className="text-xs text-purple-300 bg-purple-500/10 px-2 py-1 rounded uppercase tracking-wide">
                                {result.isChunk ? `Chunk ${result.metadata?.chunk_index ?? ''}` : (result.modality || 'semantic')}
                              </span>
                            </div>
                            <p className="text-xs text-gray-500 truncate">{result.path || 'No storage path available'}</p>
                            {result.metadata?.file_id && (
                              <p className="text-[11px] text-gray-600 mt-1">File ID: {result.metadata.file_id}</p>
                            )}
                            {result.description && (
                              <p className="text-sm text-gray-300 mt-2 line-clamp-3">
                                {result.description}
                              </p>
                            )}
                            <div className="flex flex-wrap items-center gap-3 mt-2 text-xs text-gray-400">
                              {typeof result.similarity === 'number' && (
                                <span className="flex items-center gap-1">
                                  <Sparkles className="w-3 h-3" />
                                  {(result.similarity * 100).toFixed(1)}% match
                                </span>
                              )}
                              {result.metadata?.collection && (
                                <span className="uppercase tracking-wide text-[11px] text-gray-500">
                                  {result.metadata.collection}
                                </span>
                              )}
                            </div>
                            <div className="flex items-center gap-2 mt-3">
                              <motion.button
                                onClick={() => {
                                  const target = result.downloadPath || result.path
                                  if (target) handleDownload(target)
                                }}
                                whileHover={{ scale: 1.05 }}
                                whileTap={{ scale: 0.95 }}
                                disabled={!result.downloadPath && !result.path}
                                className="px-3 py-1.5 bg-purple-600/20 text-purple-400 text-xs font-medium rounded hover:bg-purple-600/30 transition-colors flex items-center gap-1 disabled:opacity-50 disabled:cursor-not-allowed"
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
      </motion.div>
    </div>
  )
}

export default VisualizationPage
