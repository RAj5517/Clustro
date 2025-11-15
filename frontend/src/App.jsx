import { useState } from 'react'
import Navigation from './components/Navigation'
import HomePage from './pages/HomePage'
import UploadPage from './pages/UploadPage'
import StatsPage from './pages/StatsPage'
import VisualizationPage from './pages/VisualizationPage'

function App() {
  const [currentPage, setCurrentPage] = useState('home')

  const handleNavigate = (page) => {
    setCurrentPage(page)
  }

  const renderPage = () => {
    switch (currentPage) {
      case 'home':
        return <HomePage onNavigate={handleNavigate} />
      case 'upload':
        return <UploadPage onNavigate={handleNavigate} />
      case 'stats':
        return <StatsPage onNavigate={handleNavigate} />
      case 'visualization':
        return <VisualizationPage onNavigate={handleNavigate} />
      default:
        return <HomePage onNavigate={handleNavigate} />
    }
  }

  return (
    <div className="min-h-screen">
      <div className="p-4 md:p-8">
        <div className="max-w-7xl mx-auto">
          <Navigation currentPage={currentPage} onNavigate={handleNavigate} />
          {renderPage()}
        </div>
      </div>
    </div>
  )
}

export default App
