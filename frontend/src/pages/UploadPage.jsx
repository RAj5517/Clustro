import { useState } from 'react'
import { motion } from 'framer-motion'
import DataInput from '../components/DataInput'
import { uploadData } from '../services/api'
import { Sparkles } from 'lucide-react'

const UploadPage = ({ onNavigate }) => {
  const [newSubmission, setNewSubmission] = useState(null)
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState(null)

  const handleDataSubmit = async (submission) => {
    setNewSubmission(submission)
    setIsLoading(true)
    setError(null)

    try {
      // Upload data to backend
      const response = await uploadData(submission)
      
      // Clear new submission after animation
      setTimeout(() => setNewSubmission(null), 3000)
    } catch (err) {
      console.error('Failed to upload data:', err)
      setError(`Failed to upload data: ${err.message}`)
      setTimeout(() => setNewSubmission(null), 3000)
    } finally {
      setIsLoading(false)
    }
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
              Upload Data
            </h2>
          </div>
          <p className="text-gray-400">
            Upload your files or folders and let the intelligent system process them
          </p>
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

        <div className="grid grid-cols-1 gap-6 mb-16">
          <DataInput onDataSubmit={handleDataSubmit} />
        </div>

        {/* Live Update Indicator */}
        {newSubmission && (
          <motion.div
            initial={{ opacity: 0, y: 20, scale: 0.8 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: -20, scale: 0.8 }}
            whileHover={{ scale: 1.05 }}
            className="fixed bottom-6 right-6 glass-strong rounded-lg p-4 shadow-2xl hover-lift border-purple-500/30"
          >
            <div className="flex items-center gap-3">
              <motion.div
                animate={{ rotate: 360 }}
                transition={{ duration: 1, repeat: Infinity, ease: "linear" }}
              >
                <Sparkles className="w-5 h-5 text-yellow-400" />
              </motion.div>
              <div>
                <p className="font-semibold text-sm">
                  Processing {newSubmission.files?.length || 0} file{newSubmission.files?.length !== 1 ? 's' : ''}...
                </p>
                <p className="text-xs text-gray-400">Updating database structure</p>
              </div>
            </div>
          </motion.div>
        )}
      </motion.div>
    </div>
  )
}

export default UploadPage

