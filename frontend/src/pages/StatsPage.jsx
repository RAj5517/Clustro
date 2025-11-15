import { useState, useEffect } from 'react'
import { motion } from 'framer-motion'
import { BarChart3, Database, FileText, Folder, Sparkles, TrendingUp, Clock, Zap } from 'lucide-react'
import { getStats, getComparisonStats } from '../services/api'

const StatsPage = ({ onNavigate }) => {
  const [stats, setStats] = useState(null)
  const [comparisonData, setComparisonData] = useState(null)
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

  // Mock comparison data - performance metrics over time
  const mockComparisonData = {
    realWorld: {
      label: 'Real-World System',
      color: 'rgb(59, 130, 246)', // blue
      timeSeries: [
        { time: '00:00', value: 120 },
        { time: '04:00', value: 135 },
        { time: '08:00', value: 180 },
        { time: '12:00', value: 220 },
        { time: '16:00', value: 200 },
        { time: '20:00', value: 165 },
        { time: '24:00', value: 130 }
      ],
      averageTime: '185ms',
      peakTime: '220ms',
      efficiency: '72%'
    },
    auraverse: {
      label: 'Clustro System',
      color: 'rgb(168, 85, 247)', // purple
      timeSeries: [
        { time: '00:00', value: 45 },
        { time: '04:00', value: 50 },
        { time: '08:00', value: 65 },
        { time: '12:00', value: 75 },
        { time: '16:00', value: 70 },
        { time: '20:00', value: 55 },
        { time: '24:00', value: 48 }
      ],
      averageTime: '58ms',
      peakTime: '75ms',
      efficiency: '94%'
    }
  }

  useEffect(() => {
    // TODO: Replace with real API call when backend is ready
    // fetchStatsData()
    // fetchComparisonData()
    setStats(mockStats)
    setComparisonData(mockComparisonData)
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

  const fetchComparisonData = async () => {
    try {
      const data = await getComparisonStats()
      setComparisonData(data)
    } catch (err) {
      console.error('Failed to fetch comparison data:', err)
      setComparisonData(mockComparisonData)
    }
  }

  // Render a simple line graph
  const renderTimeGraph = (data, color) => {
    const maxValue = Math.max(...data.timeSeries.map(d => d.value)) * 1.1 // Add 10% padding
    const graphHeight = 200
    const graphPadding = { top: 10, right: 20, bottom: 30, left: 50 }

    return (
      <div className="relative w-full overflow-hidden" style={{ height: `${graphHeight + graphPadding.bottom}px` }}>
        <svg 
          width="100%" 
          height={graphHeight} 
          className="absolute"
          style={{ 
            left: `${graphPadding.left}px`,
            top: `${graphPadding.top}px`,
            right: `${graphPadding.right}px`,
            width: `calc(100% - ${graphPadding.left + graphPadding.right}px)`
          }}
          viewBox={`0 0 ${1000} ${graphHeight}`}
          preserveAspectRatio="xMidYMid meet"
        >
          {/* Grid lines */}
          {[0, 1, 2, 3, 4].map((i) => {
            const yPos = (i / 4) * graphHeight
            return (
              <line
                key={i}
                x1="0"
                y1={yPos}
                x2="1000"
                y2={yPos}
                stroke="rgba(255, 255, 255, 0.05)"
                strokeWidth="1"
              />
            )
          })}
          {/* Line graph */}
          <polyline
            fill="none"
            stroke={color}
            strokeWidth="4"
            strokeLinecap="round"
            strokeLinejoin="round"
            points={data.timeSeries.map((point, index, arr) => {
              const x = (index / (arr.length - 1)) * 1000
              const y = graphHeight - (point.value / maxValue) * graphHeight
              return `${x},${y}`
            }).join(' ')}
          />
          {/* Data points */}
          {data.timeSeries.map((point, index, arr) => {
            const x = (index / (arr.length - 1)) * 1000
            const y = graphHeight - (point.value / maxValue) * graphHeight
            return (
              <g key={index}>
                <circle
                  cx={x}
                  cy={y}
                  r="6"
                  fill={color}
                  className="hover:opacity-80 transition-all cursor-pointer"
                />
                <circle
                  cx={x}
                  cy={y}
                  r="3"
                  fill="white"
                />
              </g>
            )
          })}
        </svg>
        {/* X-axis labels */}
        <div 
          className="absolute bottom-0 flex justify-between text-xs text-gray-500"
          style={{ 
            left: `${graphPadding.left}px`,
            right: `${graphPadding.right}px`
          }}
        >
          {data.timeSeries.map((point, index) => (
            index % 2 === 0 && (
              <span key={index}>{point.time}</span>
            )
          ))}
        </div>
        {/* Y-axis labels */}
        <div 
          className="absolute left-0 top-0 bottom-0 flex flex-col justify-between text-xs text-gray-500"
          style={{ 
            width: `${graphPadding.left}px`,
            paddingTop: `${graphPadding.top}px`,
            paddingBottom: `${graphPadding.bottom}px`
          }}
        >
          <span className="text-right pr-2">{Math.round(maxValue)}ms</span>
          <span className="text-right pr-2">{Math.round(maxValue / 2)}ms</span>
          <span className="text-right pr-2">0ms</span>
        </div>
      </div>
    )
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

        {/* Comparison Section - Side by Side */}
        {comparisonData && (
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.6 }}
            className="mb-8"
          >
            <div className="text-center mb-6">
              <h3 className="text-2xl font-bold mb-2 bg-gradient-to-r from-purple-400 to-pink-400 bg-clip-text text-transparent">
                Performance Comparison
              </h3>
              <p className="text-gray-400">Real-World System vs Clustro System</p>
            </div>

            {/* Side by Side Graphs */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
              {/* Real-World System */}
              <motion.div
                initial={{ opacity: 0, x: -20 }}
                animate={{ opacity: 1, x: 0 }}
                transition={{ delay: 0.7 }}
                className="glass rounded-xl p-6 overflow-hidden"
              >
                <div className="flex items-center justify-between mb-4">
                  <div className="flex items-center gap-3">
                    <div className="w-3 h-3 rounded-full" style={{ backgroundColor: comparisonData.realWorld.color }} />
                    <h4 className="text-lg font-bold text-white">{comparisonData.realWorld.label}</h4>
                  </div>
                  <Clock className="w-5 h-5 text-blue-400" />
                </div>
                <div className="mb-4 overflow-hidden">
                  {renderTimeGraph(comparisonData.realWorld, comparisonData.realWorld.color)}
                </div>
                <div className="grid grid-cols-3 gap-4 pt-4 border-t border-gray-700">
                  <div>
                    <p className="text-xs text-gray-500 mb-1">Avg Time</p>
                    <p className="text-lg font-bold text-blue-400">{comparisonData.realWorld.averageTime}</p>
                  </div>
                  <div>
                    <p className="text-xs text-gray-500 mb-1">Peak Time</p>
                    <p className="text-lg font-bold text-blue-400">{comparisonData.realWorld.peakTime}</p>
                  </div>
                  <div>
                    <p className="text-xs text-gray-500 mb-1">Efficiency</p>
                    <p className="text-lg font-bold text-blue-400">{comparisonData.realWorld.efficiency}</p>
                  </div>
                </div>
              </motion.div>

              {/* Clustro System */}
              <motion.div
                initial={{ opacity: 0, x: 20 }}
                animate={{ opacity: 1, x: 0 }}
                transition={{ delay: 0.8 }}
                className="glass rounded-xl p-6 border-2 border-purple-500/30 overflow-hidden"
              >
                <div className="flex items-center justify-between mb-4">
                  <div className="flex items-center gap-3">
                    <div className="w-3 h-3 rounded-full" style={{ backgroundColor: comparisonData.auraverse.color }} />
                    <h4 className="text-lg font-bold text-white">{comparisonData.auraverse.label}</h4>
                  </div>
                  <Zap className="w-5 h-5 text-purple-400" />
                </div>
                <div className="mb-4 overflow-hidden">
                  {renderTimeGraph(comparisonData.auraverse, comparisonData.auraverse.color)}
                </div>
                <div className="grid grid-cols-3 gap-4 pt-4 border-t border-gray-700">
                  <div>
                    <p className="text-xs text-gray-500 mb-1">Avg Time</p>
                    <p className="text-lg font-bold text-purple-400">{comparisonData.auraverse.averageTime}</p>
                  </div>
                  <div>
                    <p className="text-xs text-gray-500 mb-1">Peak Time</p>
                    <p className="text-lg font-bold text-purple-400">{comparisonData.auraverse.peakTime}</p>
                  </div>
                  <div>
                    <p className="text-xs text-gray-500 mb-1">Efficiency</p>
                    <p className="text-lg font-bold text-purple-400">{comparisonData.auraverse.efficiency}</p>
                  </div>
                </div>
              </motion.div>
            </div>

            {/* Improvement Metrics */}
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.9 }}
              className="glass rounded-xl p-6 bg-gradient-to-r from-purple-900/20 to-pink-900/20 border border-purple-500/30"
            >
              <h4 className="text-lg font-bold mb-4 text-purple-400 flex items-center gap-2">
                <TrendingUp className="w-5 h-5" />
                Performance Improvement
              </h4>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                <div className="text-center">
                  <p className="text-3xl font-bold text-green-400 mb-1">
                    {(comparisonData.realWorld.averageTime.match(/\d+/)?.[0] / comparisonData.auraverse.averageTime.match(/\d+/)?.[0]).toFixed(1)}x
                  </p>
                  <p className="text-sm text-gray-400">Faster Average Response</p>
                </div>
                <div className="text-center">
                  <p className="text-3xl font-bold text-green-400 mb-1">
                    {(comparisonData.realWorld.peakTime.match(/\d+/)?.[0] / comparisonData.auraverse.peakTime.match(/\d+/)?.[0]).toFixed(1)}x
                  </p>
                  <p className="text-sm text-gray-400">Faster Peak Performance</p>
                </div>
                <div className="text-center">
                  <p className="text-3xl font-bold text-green-400 mb-1">
                    +{parseInt(comparisonData.auraverse.efficiency) - parseInt(comparisonData.realWorld.efficiency)}%
                  </p>
                  <p className="text-sm text-gray-400">Better Efficiency</p>
                </div>
              </div>
            </motion.div>
          </motion.div>
        )}

        {/* Storage Breakdown */}
        {stats?.storageBreakdown && (
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 1.0 }}
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

        {/* Note about endpoints */}
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 1.1 }}
          className="glass rounded-xl p-4 bg-blue-900/20 border border-blue-500/30"
        >
          <p className="text-sm text-blue-300">
            📊 <strong>Note:</strong> Currently showing static comparison data. Backend endpoints ready:
            <br />
            <code className="bg-black/50 px-2 py-1 rounded">GET /api/stats</code> - General statistics
            <br />
            <code className="bg-black/50 px-2 py-1 rounded">GET /api/stats/comparison</code> - Performance comparison data
          </p>
        </motion.div>
      </motion.div>
    </div>
  )
}

export default StatsPage

