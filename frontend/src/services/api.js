/**
 * API Service for Clustro
 * 
 * This file contains all API endpoints for communicating with the backend.
 * Update the BASE_URL to match your backend server URL.
 */

const BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000/api'

/**
 * Upload files and metadata to the backend
 * @param {Object} submission - The submission object from DataInput component
 * @param {File[]} submission.files - Array of files to upload (any type)
 * @param {string} submission.metadata - Optional metadata/comments
 * @returns {Promise<Object>} - Response from backend with database structure update
 */
export const uploadData = async (submission) => {
  try {
    const formData = new FormData()

    // Add metadata if provided
    if (submission.metadata) {
      formData.append('metadata', submission.metadata)
    }

    // Add all files
    submission.files.forEach((file) => {
      formData.append('files', file)
    })

    // Upload to single unified endpoint
    const response = await fetch(`${BASE_URL}/upload`, {
      method: 'POST',
      body: formData
    })

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`)
    }

    return await response.json()
  } catch (error) {
    console.error('Error uploading data:', error)
    throw error
  }
}

/**
 * Fetch the current database visualization state from the backend
 * @returns {Promise<Object>} - Database state with tables, collections, and mediaDirectories
 */
export const getDatabaseState = async () => {
  try {
    const response = await fetch(`${BASE_URL}/database/state`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      }
    })

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`)
    }

    const data = await response.json()
    
    // Expected format:
    // {
    //   tables: [],
    //   collections: [],
    //   mediaDirectories: []
    // }
    
    return data
  } catch (error) {
    console.error('Error fetching database state:', error)
    throw error
  }
}

/**
 * Get a specific database entity (table, collection, or media directory)
 * @param {string} type - Type of entity: 'table', 'collection', or 'media'
 * @param {string} id - ID or name of the entity
 * @returns {Promise<Object>} - Entity details
 */
export const getDatabaseEntity = async (type, id) => {
  try {
    const response = await fetch(`${BASE_URL}/database/${type}/${id}`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      }
    })

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`)
    }

    return await response.json()
  } catch (error) {
    console.error('Error fetching database entity:', error)
    throw error
  }
}

/**
 * Get statistics about the database
 * @returns {Promise<Object>} - Statistics object with file counts, sizes, etc.
 */
export const getStats = async () => {
  try {
    const response = await fetch(`${BASE_URL}/stats`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      }
    })

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`)
    }

    return await response.json()
  } catch (error) {
    console.error('Error fetching stats:', error)
    throw error
  }
}

/**
 * Get performance comparison statistics (Real-World vs Clustro)
 * @returns {Promise<Object>} - Comparison data with time series graphs
 */
export const getComparisonStats = async () => {
  try {
    const response = await fetch(`${BASE_URL}/stats/comparison`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      }
    })

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`)
    }

    return await response.json()
  } catch (error) {
    console.error('Error fetching comparison stats:', error)
    throw error
  }
}

/**
 * Get file tree structure for visualization
 * @returns {Promise<Object>} - File tree structure with folders and files
 */
export const getVisualizationData = async () => {
  try {
    const response = await fetch(`${BASE_URL}/visualization`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      }
    })

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`)
    }

    return await response.json()
  } catch (error) {
    console.error('Error fetching visualization data:', error)
    throw error
  }
}

/**
 * Search files and folders
 * @param {string} query - Search query string
 * @returns {Promise<Array>} - Array of search results
 */
export const searchFiles = async (query) => {
  try {
    const response = await fetch(`${BASE_URL}/search?q=${encodeURIComponent(query)}`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      }
    })

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`)
    }

    return await response.json()
  } catch (error) {
    console.error('Error searching files:', error)
    throw error
  }
}

/**
 * Search videos by description
 * @param {string} description - Natural language description of the video content
 * @returns {Promise<Array>} - Array of video search results
 */
export const searchVideos = async (description) => {
  try {
    const response = await fetch(`${BASE_URL}/search/videos?description=${encodeURIComponent(description)}`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      }
    })

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`)
    }

    return await response.json()
  } catch (error) {
    console.error('Error searching videos:', error)
    throw error
  }
}

/**
 * Download a file
 * @param {string} filePath - Path to the file to download
 * @returns {Promise<Blob>} - File blob for download
 */
export const downloadFile = async (filePath) => {
  try {
    const response = await fetch(`${BASE_URL}/download?path=${encodeURIComponent(filePath)}`, {
      method: 'GET',
    })

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`)
    }

    const blob = await response.blob()
    
    // Create download link
    const url = window.URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = filePath.split('/').pop() || 'download'
    document.body.appendChild(a)
    a.click()
    window.URL.revokeObjectURL(url)
    document.body.removeChild(a)

    return blob
  } catch (error) {
    console.error('Error downloading file:', error)
    throw error
  }
}

