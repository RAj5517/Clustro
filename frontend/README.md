# AURAverse - Intelligent Multi-Modal Storage System

A modern, intelligent storage system frontend built with React, Tailwind CSS, and Framer Motion.

## Features

- **Unified Input Interface**: Single interface with tabs for Media Files and Structured Data (JSON)
- **Intelligent Processing**: Automatically categorizes media files and determines SQL vs NoSQL storage for JSON data
- **Live Database Visualization**: Real-time visualization of database structure with smooth animations
- **Beautiful UI**: Modern glassmorphism design with gradient backgrounds and smooth transitions

## Tech Stack

- React 18
- Vite
- Tailwind CSS
- Framer Motion (animations)
- Lucide React (icons)

## Getting Started

1. Install dependencies:
```bash
npm install
```

2. Start the development server:
```bash
npm run dev
```

3. Build for production:
```bash
npm run build
```

## Design Decisions

### Unified Input Interface
The frontend uses a **unified interface with tabs** rather than separate fields. This approach:
- Provides a cleaner, more intuitive user experience
- Reduces cognitive load
- Allows for easy switching between data types
- Maintains consistency in the UI

### Database Visualization
The visualization component shows:
- SQL Tables with columns and relationships
- NoSQL Collections with document counts and schemas
- Media Directories organized by category
- Live updates with highlighting when new data is submitted

## Project Structure

```
src/
  ├── components/
  │   ├── DataInput.jsx          # Unified input interface
  │   └── DatabaseVisualization.jsx  # Database structure visualization
  ├── App.jsx                     # Main application component
  ├── main.jsx                    # Entry point
  └── index.css                   # Global styles
```

