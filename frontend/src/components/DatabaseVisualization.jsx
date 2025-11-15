import { useState, useEffect } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { Database, Table, FileText, Folder, ArrowRight, Sparkles } from 'lucide-react'

const DatabaseVisualization = ({ databaseState, newSubmission }) => {
  const [highlightedItem, setHighlightedItem] = useState(null)

  useEffect(() => {
    if (newSubmission) {
      // Highlight the newly added item
      const itemId = `${newSubmission.type}-${Date.now()}`
      setHighlightedItem(itemId)
      setTimeout(() => setHighlightedItem(null), 3000)
    }
  }, [newSubmission])

  const renderTable = (table, index) => {
    const isHighlighted = highlightedItem === `table-${index}`
    return (
      <motion.div
        key={index}
        initial={{ opacity: 0, scale: 0.8 }}
        animate={{ 
          opacity: 1, 
          scale: 1,
          boxShadow: isHighlighted ? '0 0 20px rgba(168, 85, 247, 0.6)' : 'none'
        }}
        whileHover={{ scale: 1.03, y: -4 }}
        transition={{ delay: index * 0.1 }}
        className={`glass rounded-xl p-4 hover-lift cursor-pointer transition-all duration-300 hover:border-purple-500/50 ${isHighlighted ? 'ring-2 ring-purple-400 pulse-glow' : ''}`}
      >
        <div className="flex items-center gap-2 mb-3">
          <Table className="w-5 h-5 text-blue-400" />
          <h3 className="font-bold text-lg">{table.name}</h3>
          <span className="text-xs text-gray-400">({table.type})</span>
        </div>
        <div className="space-y-2">
          {table.columns.map((col, colIndex) => (
            <motion.div
              key={colIndex}
              initial={{ opacity: 0, x: -10 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: index * 0.1 + colIndex * 0.05 }}
              className="flex items-center justify-between text-sm bg-black/40 rounded p-2 hover:bg-black/60 transition-colors duration-200"
            >
              <span className="font-mono text-purple-300">{col.name}</span>
              <span className="text-xs text-gray-400">{col.type}</span>
            </motion.div>
          ))}
        </div>
        {table.relationships && table.relationships.length > 0 && (
          <div className="mt-3 pt-3 border-t border-gray-600">
            <p className="text-xs text-gray-400 mb-2">Relationships:</p>
            {table.relationships.map((rel, relIndex) => (
              <div key={relIndex} className="flex items-center gap-2 text-xs text-purple-300">
                <ArrowRight className="w-3 h-3" />
                <span>{rel.target} ({rel.type})</span>
              </div>
            ))}
          </div>
        )}
      </motion.div>
    )
  }

  const renderCollection = (collection, index) => {
    const isHighlighted = highlightedItem === `collection-${index}`
    return (
      <motion.div
        key={index}
        initial={{ opacity: 0, scale: 0.8 }}
        animate={{ 
          opacity: 1, 
          scale: 1,
          boxShadow: isHighlighted ? '0 0 20px rgba(168, 85, 247, 0.6)' : 'none'
        }}
        whileHover={{ scale: 1.03, y: -4 }}
        transition={{ delay: index * 0.1 }}
        className={`glass rounded-xl p-4 hover-lift cursor-pointer transition-all duration-300 hover:border-purple-500/50 ${isHighlighted ? 'ring-2 ring-purple-400 pulse-glow' : ''}`}
      >
        <div className="flex items-center gap-2 mb-3">
          <FileText className="w-5 h-5 text-green-400" />
          <h3 className="font-bold text-lg">{collection.name}</h3>
          <span className="text-xs text-gray-400">(NoSQL)</span>
        </div>
        <div className="text-sm text-gray-300">
          <p>Documents: {collection.count}</p>
          <div className="mt-2 space-y-1">
            {collection.schema && Object.entries(collection.schema).map(([key, value], i) => (
              <div key={i} className="flex items-center gap-2 text-xs bg-black/40 rounded p-1 hover:bg-black/60 transition-colors duration-200">
                <span className="font-mono text-purple-300">{key}:</span>
                <span className="text-gray-400">{typeof value}</span>
              </div>
            ))}
          </div>
        </div>
      </motion.div>
    )
  }

  const renderMediaDirectory = (directory, index) => {
    const isHighlighted = highlightedItem === `directory-${index}`
    return (
      <motion.div
        key={index}
        initial={{ opacity: 0, scale: 0.8 }}
        animate={{ 
          opacity: 1, 
          scale: 1,
          boxShadow: isHighlighted ? '0 0 20px rgba(168, 85, 247, 0.6)' : 'none'
        }}
        whileHover={{ scale: 1.03, y: -4 }}
        transition={{ delay: index * 0.1 }}
        className={`glass rounded-xl p-4 hover-lift cursor-pointer transition-all duration-300 hover:border-purple-500/50 ${isHighlighted ? 'ring-2 ring-purple-400 pulse-glow' : ''}`}
      >
        <div className="flex items-center gap-2 mb-3">
          <Folder className="w-5 h-5 text-yellow-400" />
          <h3 className="font-bold text-lg">{directory.name}</h3>
        </div>
        <div className="space-y-2">
          <p className="text-sm text-gray-400">Category: {directory.category}</p>
          <p className="text-sm text-gray-300">Files: {directory.files.length}</p>
          <div className="flex flex-wrap gap-2 mt-2">
            {directory.files.slice(0, 5).map((file, fileIndex) => (
              <motion.div
                key={fileIndex}
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                transition={{ delay: index * 0.1 + fileIndex * 0.05 }}
                className="text-xs bg-black/50 rounded px-2 py-1 hover:bg-black/70 transition-colors duration-200"
              >
                {file.name}
              </motion.div>
            ))}
            {directory.files.length > 5 && (
              <span className="text-xs text-gray-400">+{directory.files.length - 5} more</span>
            )}
          </div>
        </div>
      </motion.div>
    )
  }

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      whileHover={{ 
        boxShadow: '0 0 40px rgba(236, 72, 153, 0.6), 0 0 60px rgba(236, 72, 153, 0.4), 0 0 80px rgba(236, 72, 153, 0.2), 0 4px 20px rgba(0, 0, 0, 0.5)'
      }}
      className="glass rounded-2xl p-6 glow-container-pink glow-animated-pink transition-all duration-300"
    >
      <div className="flex items-center gap-2 mb-6">
        <motion.div
          animate={{ rotate: [0, 360] }}
          transition={{ duration: 20, repeat: Infinity, ease: "linear" }}
        >
          <Database className="w-6 h-6 text-purple-400" />
        </motion.div>
        <h2 className="text-2xl font-bold bg-gradient-to-r from-purple-400 to-pink-400 bg-clip-text text-transparent">
          Database Structure
        </h2>
        <span className="text-xs text-gray-500 bg-gray-800/50 px-2 py-1 rounded hover:bg-gray-700/50 transition-colors">
          (Simulated)
        </span>
        {newSubmission && (
          <motion.div
            initial={{ scale: 0 }}
            animate={{ scale: 1 }}
            className="ml-auto"
          >
            <Sparkles className="w-5 h-5 text-yellow-400 animate-pulse" />
          </motion.div>
        )}
      </div>

      {databaseState.tables && databaseState.tables.length > 0 && (
        <div className="mb-6">
          <h3 className="text-lg font-semibold mb-4 text-blue-400 flex items-center gap-2">
            <Table className="w-5 h-5" />
            SQL Tables
          </h3>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {databaseState.tables.map((table, index) => renderTable(table, index))}
          </div>
        </div>
      )}

      {databaseState.collections && databaseState.collections.length > 0 && (
        <div className="mb-6">
          <h3 className="text-lg font-semibold mb-4 text-green-400 flex items-center gap-2">
            <FileText className="w-5 h-5" />
            NoSQL Collections
          </h3>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {databaseState.collections.map((collection, index) => renderCollection(collection, index))}
          </div>
        </div>
      )}

      {databaseState.mediaDirectories && databaseState.mediaDirectories.length > 0 && (
        <div>
          <h3 className="text-lg font-semibold mb-4 text-yellow-400 flex items-center gap-2">
            <Folder className="w-5 h-5" />
            Media Directories
          </h3>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {databaseState.mediaDirectories.map((directory, index) => renderMediaDirectory(directory, index))}
          </div>
        </div>
      )}

      {(!databaseState.tables || databaseState.tables.length === 0) &&
       (!databaseState.collections || databaseState.collections.length === 0) &&
       (!databaseState.mediaDirectories || databaseState.mediaDirectories.length === 0) && (
        <div className="text-center py-12 text-gray-400">
          <Database className="w-16 h-16 mx-auto mb-4 opacity-50" />
          <p>No database structure yet. Submit data to see it appear here!</p>
        </div>
      )}
    </motion.div>
  )
}

export default DatabaseVisualization

