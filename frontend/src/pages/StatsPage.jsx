import { useState, useEffect } from 'react'
import { motion } from 'framer-motion'
import { BarChart3, Database, FileText, Folder, Sparkles, TrendingUp } from 'lucide-react'
import { getStats } from '../services/api'

const StatsPage = ({ onNavigate }) => {
  const [stats, setStats] = useState(null)
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState(null)

  // Static mock data for now
  const mockStats = {
    totalFiles: 1247,
    totalSize: '2.4 GB',
    tables: 12,
    collections: 8,
    mediaDirectories: 5,
    recentUploads: 23,
    storageBreakdown: {
      images: '1.2 GB',
      videos: '800 MB',
      documents: '300 MB',
      other: '100 MB'
    }
  }

  useEffect(() => {
    // TODO: Replace with real API call when backend is ready
    // fetchStatsData()
    setStats(mockStats)
  }, [])

  const fetchStatsData = async () => {
    try {
      setIsLoading(true)
      const data = await getStats()
      setStats(data)
      setError(null)
    } catch (err) {
      console.error('Failed to fetch stats:', err)
      setError('Failed to load statistics. Showing static data.')
      setStats(mockStats)
    } finally {
      setIsLoading(false)
    }
  }

  const statCards = [
    {
      title: 'Total Files',
      value: stats?.totalFiles || 0,
      icon: FileText,
      color: 'text-blue-400',
      bgColor: 'bg-blue-500/10'
    },
    {
      title: 'Total Size',
      value: stats?.totalSize || '0 GB',
      icon: Database,
      color: 'text-purple-400',
      bgColor: 'bg-purple-500/10'
    },
    {
      title: 'SQL Tables',
      value: stats?.tables || 0,
      icon: BarChart3,
      color: 'text-green-400',
      bgColor: 'bg-green-500/10'
    },
    {
      title: 'NoSQL Collections',
      value: stats?.collections || 0,
      icon: Folder,
      color: 'text-yellow-400',
      bgColor: 'bg-yellow-500/10'
    },
    {
      title: 'Media Directories',
      value: stats?.mediaDirectories || 0,
      icon: Folder,
      color: 'text-pink-400',
      bgColor: 'bg-pink-500/10'
    },
    {
      title: 'Recent Uploads',
      value: stats?.recentUploads || 0,
      icon: TrendingUp,
      color: 'text-cyan-400',
      bgColor: 'bg-cyan-500/10'
    }
  ]

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
              Statistics
            </h2>
          </div>
          <p className="text-gray-400">
            Overview of your database and storage statistics
          </p>
        </motion.div>

        {error && (
          <motion.div
            initial={{ opacity: 0, y: -20 }}
            animate={{ opacity: 1, y: 0 }}
            className="mb-6 p-4 bg-yellow-900/20 border border-yellow-500/50 rounded-lg text-yellow-300 text-sm"
          >
            ⚠️ {error}
          </motion.div>
        )}

        {/* Stats Cards Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-8">
          {statCards.map((card, index) => {
            const Icon = card.icon
            return (
              <motion.div
                key={index}
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: index * 0.1 }}
                whileHover={{ scale: 1.05, y: -5 }}
                className={`glass rounded-xl p-6 hover-lift ${card.bgColor} border border-gray-700`}
              >
                <div className="flex items-center justify-between mb-4">
                  <Icon className={`w-8 h-8 ${card.color}`} />
                  <span className="text-xs text-gray-500">STATIC DATA</span>
                </div>
                <h3 className="text-sm font-semibold text-gray-400 mb-2">{card.title}</h3>
                <p className="text-3xl font-bold text-white">{card.value}</p>
              </motion.div>
            )
          })}
        </div>

        {/* Storage Breakdown */}
        {stats?.storageBreakdown && (
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.6 }}
            className="glass rounded-xl p-6 mb-8"
          >
            <h3 className="text-xl font-bold mb-6 text-purple-400">Storage Breakdown</h3>
            <div className="space-y-4">
              {Object.entries(stats.storageBreakdown).map(([type, size], index) => (
                <div key={index} className="flex items-center justify-between">
                  <span className="text-gray-300 capitalize">{type}</span>
                  <span className="text-purple-400 font-semibold">{size}</span>
                </div>
              ))}
            </div>
          </motion.div>
        )}

        {/* Note about static data */}
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 0.8 }}
          className="glass rounded-xl p-4 bg-blue-900/20 border border-blue-500/30"
        >
          <p className="text-sm text-blue-300">
            📊 <strong>Note:</strong> Currently showing static data. Backend endpoint <code className="bg-black/50 px-2 py-1 rounded">GET /api/stats</code> is ready for integration.
          </p>
        </motion.div>
      </motion.div>
    </div>
  )
}

export default StatsPage

