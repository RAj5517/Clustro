import { useEffect } from 'react'
import { motion } from 'framer-motion'
import Hero from '../components/Hero'
import HowItWorks from '../components/HowItWorks'
import Footer from '../components/Footer'

const HomePage = ({ onNavigate }) => {
  useEffect(() => {
    // Listen for navigation event from Hero button
    const handleNavigate = (e) => {
      if (e.detail === 'upload') {
        onNavigate('upload')
      }
    }
    window.addEventListener('navigate', handleNavigate)
    return () => window.removeEventListener('navigate', handleNavigate)
  }, [onNavigate])

  return (
    <div className="min-h-screen">
      {/* Hero Section */}
      <Hero />

      {/* How It Works Section */}
      <div id="how-it-works">
        <HowItWorks />
      </div>

      {/* Footer Section */}
      <Footer />
    </div>
  )
}

export default HomePage

