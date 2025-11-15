import { useState, useRef } from 'react'
import { motion } from 'framer-motion'
import { Upload, Sparkles, X } from 'lucide-react'

const DataInput = ({ onDataSubmit }) => {
  const [dragActive, setDragActive] = useState(false)
  const [files, setFiles] = useState([])
  const [metadata, setMetadata] = useState('')
  const fileInputRef = useRef(null)

  const handleDrag = (e) => {
    e.preventDefault()
    e.stopPropagation()
    if (e.type === 'dragenter' || e.type === 'dragover') {
      setDragActive(true)
    } else if (e.type === 'dragleave') {
      setDragActive(false)
    }
  }

  const handleDrop = (e) => {
    e.preventDefault()
    e.stopPropagation()
    setDragActive(false)

    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      const droppedFiles = Array.from(e.dataTransfer.files)
      setFiles(prev => [...prev, ...droppedFiles])
    }
  }

  const handleFileInput = (e) => {
    if (e.target.files) {
      const selectedFiles = Array.from(e.target.files)
      setFiles(prev => [...prev, ...selectedFiles])
    }
  }

  const handleSubmit = () => {
    if (files.length > 0) {
      onDataSubmit({
        files: files,
        metadata: metadata
      })
      setFiles([])
      setMetadata('')
      // Reset input
      if (fileInputRef.current) {
        fileInputRef.current.value = ''
      }
    }
  }

  const removeFile = (index) => {
    setFiles(prev => prev.filter((_, i) => i !== index))
  }

  const getFileIcon = (file) => {
    if (file.type.startsWith('image/')) return '🖼️'
    if (file.type.startsWith('video/')) return '🎥'
    if (file.type.startsWith('audio/')) return '🎵'
    if (file.type === 'application/json' || file.name.endsWith('.json')) return '📄'
    if (file.type.includes('text') || file.name.endsWith('.txt')) return '📝'
    return '📦'
  }

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      whileHover={{ 
        boxShadow: '0 0 40px rgba(168, 85, 247, 0.6), 0 0 60px rgba(168, 85, 247, 0.4), 0 0 80px rgba(168, 85, 247, 0.2), 0 4px 20px rgba(0, 0, 0, 0.5)'
      }}
      className="glass rounded-2xl p-6 glow-container-purple glow-animated-purple transition-all duration-300"
    >
      <div className="flex items-center gap-2 mb-6">
        <motion.div
          animate={{ rotate: [0, 360] }}
          transition={{ duration: 20, repeat: Infinity, ease: "linear" }}
        >
          <Sparkles className="w-6 h-6 text-purple-400" />
        </motion.div>
        <h2 className="text-2xl font-bold bg-gradient-to-r from-purple-400 to-pink-400 bg-clip-text text-transparent">
          Intelligent Data Input
        </h2>
      </div>

      {/* Unified Drop Zone - Accepts both files and folders */}
      <motion.div
        onDragEnter={handleDrag}
        onDragLeave={handleDrag}
        onDragOver={handleDrag}
        onDrop={handleDrop}
        onClick={() => fileInputRef.current?.click()}
        whileHover={{ scale: 1.02 }}
        whileTap={{ scale: 0.98 }}
        className={`border-2 border-dashed rounded-xl p-8 text-center cursor-pointer transition-all duration-300 hover-lift ${
          dragActive
            ? 'border-purple-400 bg-purple-500/20 scale-105 glow-purple'
            : 'border-gray-600 hover:border-purple-400 hover:bg-gray-800/30 hover:shadow-lg hover:shadow-purple-500/20'
        }`}
      >
        <motion.div
          animate={dragActive ? { rotate: 360 } : { rotate: 0 }}
          transition={{ duration: 0.5 }}
        >
          <Upload className="w-12 h-12 mx-auto mb-4 text-purple-400" />
        </motion.div>
        <p className="text-lg font-semibold mb-2">
          Drop files or folder here or click to upload
        </p>
        <p className="text-sm text-gray-400">
          Supports all file types - images, videos, documents, JSON, etc. You can upload individual files or entire folders.
        </p>
        <input
          ref={fileInputRef}
          type="file"
          multiple
          webkitdirectory=""
          directory=""
          onChange={handleFileInput}
          className="hidden"
        />
      </motion.div>

      {/* Selected Files */}
      {files.length > 0 && (
        <motion.div
          initial={{ opacity: 0, height: 0 }}
          animate={{ opacity: 1, height: 'auto' }}
          className="mt-4 space-y-2 max-h-64 overflow-y-auto"
        >
          <div className="flex items-center justify-between mb-2">
            <p className="text-sm font-semibold text-gray-300">
              Selected Files ({files.length}):
            </p>
            <button
              onClick={() => {
                setFiles([])
                if (fileInputRef.current) fileInputRef.current.value = ''
              }}
              className="text-xs text-red-400 hover:text-red-300"
            >
              Clear All
            </button>
          </div>
          {files.map((file, index) => (
            <motion.div
              key={index}
              initial={{ opacity: 0, x: -10 }}
              animate={{ opacity: 1, x: 0 }}
              whileHover={{ scale: 1.02, x: 4 }}
              className="glass rounded-lg p-3 flex items-center justify-between hover-lift hover:border-purple-500/50 transition-all duration-200"
            >
              <div className="flex items-center gap-3 flex-1 min-w-0">
                <span className="text-xl">{getFileIcon(file)}</span>
                <span className="text-sm truncate" title={file.name}>{file.name}</span>
                <span className="text-xs text-gray-400 whitespace-nowrap">
                  ({(file.size / 1024).toFixed(2)} KB)
                </span>
              </div>
              <button
                onClick={(e) => {
                  e.stopPropagation()
                  removeFile(index)
                }}
                className="text-red-400 hover:text-red-300 ml-2"
              >
                <X className="w-4 h-4" />
              </button>
            </motion.div>
          ))}
        </motion.div>
      )}

      {/* Metadata Input */}
      <div className="mt-6">
        <label className="block text-sm font-semibold mb-2 text-gray-300">
          Optional Metadata/Comments
        </label>
        <input
          type="text"
          value={metadata}
          onChange={(e) => setMetadata(e.target.value)}
          placeholder="Add comments to aid in schema generation..."
                  className="w-full bg-black/50 border border-gray-700 rounded-lg p-3 text-white focus:outline-none focus:border-purple-400 focus:ring-2 focus:ring-purple-400/50 transition-all duration-300 hover:border-purple-500/50 hover:bg-black/60"
        />
      </div>

      {/* Submit Button */}
      <motion.button
        whileHover={{ scale: 1.02, boxShadow: '0 0 30px rgba(168, 85, 247, 0.6)' }}
        whileTap={{ scale: 0.98 }}
        onClick={handleSubmit}
        disabled={files.length === 0}
        className="relative w-full mt-6 bg-gradient-to-r from-purple-600 to-pink-600 text-white font-bold py-3 rounded-lg shadow-lg hover:shadow-purple-500/50 disabled:opacity-50 disabled:cursor-not-allowed transition-all duration-300 overflow-hidden group"
      >
        <motion.div
          className="absolute inset-0 bg-gradient-to-r from-purple-500 to-pink-500 opacity-0 group-hover:opacity-100"
          transition={{ duration: 0.3 }}
        />
        <span className="relative z-10">Submit Data</span>
      </motion.button>
    </motion.div>
  )
}

export default DataInput

