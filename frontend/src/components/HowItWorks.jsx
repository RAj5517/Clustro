import { motion } from 'framer-motion'
import { Upload, FileJson, Brain, Database, Sparkles, Zap, Folder, Table } from 'lucide-react'

const HowItWorks = () => {
  const features = [
    {
      icon: Upload,
      title: 'Upload Your Data',
      description: 'Simply drag and drop or select your media files (images, videos) or structured data files (JSON, TXT, DOC, etc.). The system accepts any file type.',
      color: 'text-purple-400'
    },
    {
      icon: Brain,
      title: 'Intelligent Analysis',
      description: 'Our AI-powered system automatically analyzes your data, categorizes content, and determines the optimal storage solution.',
      color: 'text-pink-400'
    },
    {
      icon: Database,
      title: 'Smart Storage Decision',
      description: 'For structured data, the system intelligently decides between SQL and NoSQL databases based on data structure and relationships.',
      color: 'text-blue-400'
    },
    {
      icon: Folder,
      title: 'Automatic Organization',
      description: 'Media files are automatically categorized and organized into appropriate directories. Related files are grouped together intelligently.',
      color: 'text-yellow-400'
    }
  ]

  const steps = [
    {
      number: '01',
      title: 'Submit Data',
      description: 'Upload media files or structured data through our unified interface'
    },
    {
      number: '02',
      title: 'AI Processing',
      description: 'System analyzes and categorizes your data automatically'
    },
    {
      number: '03',
      title: 'Storage Optimization',
      description: 'Intelligent decision-making for optimal database structure'
    },
    {
      number: '04',
      title: 'Visualization',
      description: 'View your organized database structure in real-time'
    }
  ]

  return (
    <section className="py-16 px-4 md:px-8">
      <div className="max-w-7xl mx-auto">
        {/* Section Header */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.6 }}
          className="text-center mb-16"
        >
          <div className="flex items-center justify-center gap-2 mb-4">
            <Sparkles className="w-8 h-8 text-purple-400" />
            <h2 className="text-4xl md:text-5xl font-bold bg-gradient-to-r from-purple-400 to-pink-400 bg-clip-text text-transparent">
              How It Works
            </h2>
          </div>
          <p className="text-xl text-gray-400 max-w-2xl mx-auto">
            An intelligent storage system that automatically organizes and optimizes your data
          </p>
        </motion.div>

        {/* Features Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-16">
          {features.map((feature, index) => {
            const Icon = feature.icon
            return (
              <motion.div
                key={index}
                initial={{ opacity: 0, y: 30 }}
                whileInView={{ opacity: 1, y: 0 }}
                viewport={{ once: true }}
                transition={{ duration: 0.5, delay: index * 0.1 }}
                whileHover={{ scale: 1.05, y: -5 }}
                className="glass rounded-xl p-6 hover-lift cursor-pointer"
              >
                <div className={`w-12 h-12 rounded-lg bg-gradient-to-br from-purple-600/20 to-pink-600/20 flex items-center justify-center mb-4 ${feature.color}`}>
                  <Icon className="w-6 h-6" />
                </div>
                <h3 className="text-xl font-bold mb-2 text-white">{feature.title}</h3>
                <p className="text-gray-400 text-sm leading-relaxed">{feature.description}</p>
              </motion.div>
            )
          })}
        </div>

        {/* Steps */}
        <motion.div
          initial={{ opacity: 0 }}
          whileInView={{ opacity: 1 }}
          viewport={{ once: true }}
          transition={{ duration: 0.6 }}
          className="glass rounded-2xl p-8 md:p-12"
        >
          <h3 className="text-2xl font-bold text-center mb-12 bg-gradient-to-r from-purple-400 to-pink-400 bg-clip-text text-transparent">
            Simple 4-Step Process
          </h3>
          <div className="relative">
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 relative z-10">
              {steps.map((step, index) => (
                <motion.div
                  key={index}
                  initial={{ opacity: 0, x: -20 }}
                  whileInView={{ opacity: 1, x: 0 }}
                  viewport={{ once: true }}
                  transition={{ duration: 0.5, delay: index * 0.1 }}
                  className="relative"
                >
                  <div className="flex flex-col items-center text-center">
                    <div className="w-16 h-16 rounded-full bg-gradient-to-br from-purple-600 to-pink-600 flex items-center justify-center mb-4 relative z-10">
                      <span className="text-2xl font-bold text-white">{step.number}</span>
                    </div>
                    <h4 className="text-lg font-semibold mb-2 text-white">{step.title}</h4>
                    <p className="text-sm text-gray-400">{step.description}</p>
                  </div>
                </motion.div>
              ))}
            </div>
            {/* Connecting lines - positioned between circles */}
            <div className="hidden lg:block absolute top-8 left-0 right-0 h-0.5 z-0">
              {steps.slice(0, -1).map((_, index) => {
                const startPercent = (index + 0.5) * (100 / steps.length)
                const endPercent = (index + 1.5) * (100 / steps.length)
                const width = endPercent - startPercent
                return (
                  <div
                    key={index}
                    className="absolute h-full bg-gradient-to-r from-purple-600 to-pink-600"
                    style={{
                      left: `${startPercent}%`,
                      width: `${width}%`
                    }}
                  />
                )
              })}
            </div>
          </div>
        </motion.div>
      </div>
    </section>
  )
}

export default HowItWorks

