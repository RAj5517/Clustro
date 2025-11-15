import { motion } from 'framer-motion'
import { Home, Upload, BarChart3, FolderTree, Sparkles } from 'lucide-react'

const Navigation = ({ currentPage, onNavigate }) => {
  const navItems = [
    { id: 'home', label: 'Home', icon: Home },
    { id: 'upload', label: 'Upload', icon: Upload },
    { id: 'stats', label: 'Statistics', icon: BarChart3 },
    { id: 'visualization', label: 'Visualization', icon: FolderTree }
  ]

  return (
    <nav className="mb-8">
      <div className="flex items-center justify-between">
        {/* Logo/Brand */}
        <motion.div
          initial={{ opacity: 0, x: -20 }}
          animate={{ opacity: 1, x: 0 }}
          className="flex items-center gap-2"
        >
          <motion.div
            animate={{ rotate: [0, 360] }}
            transition={{ duration: 20, repeat: Infinity, ease: "linear" }}
          >
            <Sparkles className="w-6 h-6 text-purple-400" />
          </motion.div>
          <span className="text-xl font-bold bg-gradient-to-r from-purple-400 to-pink-400 bg-clip-text text-transparent">
            Clustro
          </span>
        </motion.div>

        {/* Navigation Items */}
        <div className="flex items-center gap-2">
          {navItems.map((item) => {
            const Icon = item.icon
            const isActive = currentPage === item.id
            
            return (
              <motion.button
                key={item.id}
                onClick={() => onNavigate(item.id)}
                whileHover={{ scale: 1.05, y: -2 }}
                whileTap={{ scale: 0.95 }}
                className={`relative flex items-center gap-2 px-5 py-2.5 rounded-lg font-medium transition-all duration-300 overflow-hidden ${
                  isActive
                    ? 'text-white'
                    : 'text-gray-400 hover:text-purple-300'
                }`}
              >
                {isActive && (
                  <motion.div
                    className="absolute inset-0 bg-gradient-to-r from-purple-600 to-pink-600 rounded-lg"
                    layoutId="activeNav"
                    transition={{ type: "spring", bounce: 0.2, duration: 0.6 }}
                  />
                )}
                <Icon className={`w-4 h-4 relative z-10 ${isActive ? 'text-white' : ''}`} />
                <span className="relative z-10">{item.label}</span>
                {isActive && (
                  <motion.div
                    className="absolute bottom-0 left-0 right-0 h-0.5 bg-white/30"
                    initial={{ scaleX: 0 }}
                    animate={{ scaleX: 1 }}
                    transition={{ duration: 0.3 }}
                  />
                )}
              </motion.button>
            )
          })}
        </div>
      </div>
    </nav>
  )
}

export default Navigation

