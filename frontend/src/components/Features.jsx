import { motion } from 'framer-motion'
import { Zap, Shield, BarChart3, Globe, Lock, TrendingUp, Brain } from 'lucide-react'

const Features = () => {
  const features = [
    {
      icon: Zap,
      title: 'Lightning Fast',
      description: 'Process and organize your data in seconds with optimized algorithms and intelligent caching.',
      gradient: 'from-yellow-500 to-orange-500'
    },
    {
      icon: Brain,
      title: 'AI-Powered',
      description: 'Advanced machine learning models analyze and categorize your data automatically.',
      gradient: 'from-purple-500 to-pink-500'
    },
    {
      icon: Shield,
      title: 'Secure & Private',
      description: 'Your data is encrypted and stored securely with enterprise-grade security measures.',
      gradient: 'from-blue-500 to-cyan-500'
    },
    {
      icon: BarChart3,
      title: 'Smart Analytics',
      description: 'Get insights into your data structure and optimize storage based on usage patterns.',
      gradient: 'from-green-500 to-emerald-500'
    },
    {
      icon: Globe,
      title: 'Multi-Format Support',
      description: 'Supports any file type - images, videos, documents, JSON, and more.',
      gradient: 'from-indigo-500 to-purple-500'
    },
    {
      icon: TrendingUp,
      title: 'Scalable',
      description: 'Grows with your needs - from small projects to enterprise-level data management.',
      gradient: 'from-red-500 to-pink-500'
    }
  ]

  return (
    <section className="py-16 px-4 md:px-8 bg-black">
      <div className="max-w-7xl mx-auto">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.6 }}
          className="text-center mb-16"
        >
          <h2 className="text-4xl md:text-5xl font-bold bg-gradient-to-r from-purple-400 to-pink-400 bg-clip-text text-transparent mb-4">
            Powerful Features
          </h2>
          <p className="text-xl text-gray-400 max-w-2xl mx-auto">
            Everything you need for intelligent data management
          </p>
        </motion.div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
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
                className="glass rounded-xl p-6 hover-lift cursor-pointer group"
              >
                <div className={`w-14 h-14 rounded-lg bg-gradient-to-br ${feature.gradient} flex items-center justify-center mb-4 group-hover:scale-110 transition-transform duration-300`}>
                  <Icon className="w-7 h-7 text-white" />
                </div>
                <h3 className="text-xl font-bold mb-2 text-white">{feature.title}</h3>
                <p className="text-gray-400 text-sm leading-relaxed">{feature.description}</p>
              </motion.div>
            )
          })}
        </div>
      </div>
    </section>
  )
}

export default Features

