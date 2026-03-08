"use client"
import React, { useEffect, useRef, useState, useCallback, useMemo } from "react"
import dynamic from "next/dynamic"
import { useRouter } from "next/navigation"
import { Search, X, ExternalLink, User, Briefcase, Calendar, Shield, Box, Square, MessageSquare } from "lucide-react"
import { useAuth, useAuthenticatedAxios } from "@/components/AuthContext"
import * as THREE from "three"

const ForceGraph2D = dynamic(() => import("react-force-graph-2d").then(mod => mod.default || mod), { ssr: false })
const ForceGraph3D = dynamic(() => import("react-force-graph-3d").then(mod => mod.default || mod), { ssr: false })

type GraphNode = {
  id: string
  name: string
  type: "user" | "person" | "company"
  title?: string
  is_recruiter?: boolean
  initials?: string
  logo?: string
  url?: string
  profile_url?: string
  connected_on?: string
  is_source?: boolean
  network_name?: string
  x?: number
  y?: number
  z?: number
}

type GraphLink = {
  source: string | GraphNode
  target: string | GraphNode
  label: string
}

type GraphData = {
  nodes: GraphNode[]
  links: GraphLink[]
}

type Props = {
  width?: number
  height?: number
  initialZoom?: boolean
  default3D?: boolean
  activeFilter?: string | null
}

const NODE_COLORS: Record<string, string> = {
  user: "#8B5CF6",
  person: "#06B6D4",
  company: "#F59E0B",
}

const NODE_SIZES: Record<string, number> = {
  user: 10,
  person: 5,
  company: 7,
}

const NODE_SIZES_3D: Record<string, number> = {
  user: 8,
  person: 4,
  company: 6,
}

const logoCache = new Map<string, HTMLImageElement | null>()

function loadLogo(url: string): HTMLImageElement | null {
  if (!url) return null
  if (logoCache.has(url)) return logoCache.get(url)!
  logoCache.set(url, null)
  const img = new Image()
  img.crossOrigin = "anonymous"
  img.onload = () => logoCache.set(url, img)
  img.onerror = () => logoCache.set(url, null)
  img.src = url
  return null
}

export default function Graph({ width, height, initialZoom, default3D = false, activeFilter = null }: Props) {
  const router = useRouter()
  const fgRef = useRef<any>(null)
  const { isAuthenticated } = useAuth()
  const getAuthAxios = useAuthenticatedAxios()
  const [data, setData] = useState<GraphData>({ nodes: [], links: [] })
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [searchQuery, setSearchQuery] = useState("")
  const [isFocused, setIsFocused] = useState(false)
  const [selectedNode, setSelectedNode] = useState<GraphNode | null>(null)
  const [hasInitialZoomed, setHasInitialZoomed] = useState(false)
  const [is3D, setIs3D] = useState(default3D)

  // Collect all searchable names for autocomplete (companies + people)
  const searchableNames = useMemo(() => {
    return data.nodes
      .filter(n => n.type === "company" || n.type === "person")
      .map(n => ({ name: n.name, type: n.type }))
      .sort((a, b) => a.name.localeCompare(b.name))
  }, [data.nodes])

  const suggestions = useMemo(() => {
    if (!searchQuery.trim()) return []
    const q = searchQuery.toLowerCase()
    return searchableNames
      .filter(item => item.name.toLowerCase().includes(q))
      .slice(0, 8)
  }, [searchQuery, searchableNames])

  const filteredData = useMemo<GraphData>(() => {
    let working = data

    // Apply recruiter filter from legend click
    if (activeFilter === "recruiter") {
      const recruiterIds = new Set<string>()
      const companyIds = new Set<string>()

      data.nodes.forEach(n => {
        if (n.is_recruiter) recruiterIds.add(n.id)
      })

      // Also keep the user node
      const userNode = data.nodes.find(n => n.type === "user")
      if (userNode) recruiterIds.add(userNode.id)

      // Keep companies connected to recruiters
      data.links.forEach(link => {
        const srcId = typeof link.source === "string" ? link.source : link.source.id
        const tgtId = typeof link.target === "string" ? link.target : link.target.id
        if (recruiterIds.has(srcId) && link.label === "WORKS_AT") companyIds.add(tgtId)
        if (recruiterIds.has(tgtId) && link.label === "WORKS_AT") companyIds.add(srcId)
      })

      const keepIds = new Set<string>()
      recruiterIds.forEach(id => keepIds.add(id))
      companyIds.forEach(id => keepIds.add(id))

      const nodes = data.nodes.filter(n => keepIds.has(n.id))
      const links = data.links.filter(link => {
        const srcId = typeof link.source === "string" ? link.source : link.source.id
        const tgtId = typeof link.target === "string" ? link.target : link.target.id
        return keepIds.has(srcId) && keepIds.has(tgtId)
      })
      working = { nodes, links }
    }

    // Apply search query
    const q = searchQuery.trim().toLowerCase()
    if (!q) return working

    // Match companies by name
    const matchedCompanyIds = new Set(
      working.nodes
        .filter(n => n.type === "company" && n.name.toLowerCase().includes(q))
        .map(n => n.id)
    )

    // Match people by name
    const matchedPersonIds = new Set(
      working.nodes
        .filter(n => (n.type === "person" || n.type === "user") && n.name.toLowerCase().includes(q))
        .map(n => n.id)
    )

    if (matchedCompanyIds.size === 0 && matchedPersonIds.size === 0) return working

    const keepIds = new Set<string>()

    // Always keep the user node
    const userNode = working.nodes.find(n => n.type === "user")
    if (userNode) keepIds.add(userNode.id)

    // Add matched companies and their connected people
    matchedCompanyIds.forEach(id => keepIds.add(id))
    working.links.forEach(link => {
      const srcId = typeof link.source === "string" ? link.source : link.source.id
      const tgtId = typeof link.target === "string" ? link.target : link.target.id
      if (matchedCompanyIds.has(tgtId)) keepIds.add(srcId)
      if (matchedCompanyIds.has(srcId)) keepIds.add(tgtId)
    })

    // Add matched people and their connected companies
    matchedPersonIds.forEach(id => keepIds.add(id))
    working.links.forEach(link => {
      const srcId = typeof link.source === "string" ? link.source : link.source.id
      const tgtId = typeof link.target === "string" ? link.target : link.target.id
      if (matchedPersonIds.has(srcId) && link.label === "WORKS_AT") keepIds.add(tgtId)
      if (matchedPersonIds.has(tgtId) && link.label === "WORKS_AT") keepIds.add(srcId)
    })

    const nodes = working.nodes.filter(n => keepIds.has(n.id))
    const links = working.links.filter(link => {
      const srcId = typeof link.source === "string" ? link.source : link.source.id
      const tgtId = typeof link.target === "string" ? link.target : link.target.id
      return keepIds.has(srcId) && keepIds.has(tgtId)
    })

    return { nodes, links }
  }, [data, searchQuery, activeFilter])

  useEffect(() => {
    let mounted = true
    async function load() {
      if (!isAuthenticated) {
        setError("Please log in to view your network graph.")
        setLoading(false)
        return
      }
      
      setLoading(true)
      setError(null)
      
      try {
        const axios = await getAuthAxios()
        const res = await axios.get("/api/graph/overview")
        if (!mounted) return
        setData(res.data)
      } catch (err: any) {
        if (!mounted) return
        if (err?.response?.status === 401) {
          setError("Session expired. Please log in again.")
        } else {
          setError(err?.message ?? "Failed to load graph")
        }
      } finally {
        if (mounted) setLoading(false)
      }
    }
    load()
    return () => { mounted = false }
  }, [isAuthenticated, getAuthAxios])

  const handleNodeClick = useCallback((node: any) => {
    if (is3D && fgRef.current) {
      const distance = 120
      const distRatio = 1 + distance / Math.hypot(node.x, node.y, node.z)
      fgRef.current.cameraPosition(
        { x: node.x * distRatio, y: node.y * distRatio, z: node.z * distRatio },
        node,
        1000
      )
    } else if (fgRef.current && node.x !== undefined && node.y !== undefined) {
      fgRef.current.centerAt(node.x, node.y, 600)
      fgRef.current.zoom(2.5, 600)
    }
    
    if (node.type === "company") {
      if (node.url) {
        window.open(node.url, "_blank", "noopener,noreferrer")
      }
      setSelectedNode(null)
    } else if (node.type === "person" || node.type === "user") {
      setSelectedNode(node as GraphNode)
    } else {
      setSelectedNode(null)
    }
  }, [is3D])

  const handleEngineStop = useCallback(() => {
    if (initialZoom && !hasInitialZoomed && fgRef.current && data.nodes.length > 0) {
      setHasInitialZoomed(true)
      const userNode = data.nodes.find(n => n.type === "user")
      
      if (is3D) {
        if (userNode && userNode.x !== undefined) {
          const distance = 200
          fgRef.current.cameraPosition(
            { x: distance, y: distance, z: distance },
            { x: 0, y: 0, z: 0 },
            1500
          )
        }
      } else {
        if (userNode && userNode.x !== undefined && userNode.y !== undefined) {
          fgRef.current.centerAt(userNode.x, userNode.y, 800)
          fgRef.current.zoom(2.5, 800)
        } else {
          fgRef.current.zoom(2.0, 800)
        }
      }
    }
  }, [initialZoom, hasInitialZoomed, data, is3D])

  // Pin node in place after dragging so the simulation doesn't pull it back
  const handleNodeDragEnd = useCallback((node: any) => {
    node.fx = node.x
    node.fy = node.y
    if (is3D) node.fz = node.z
  }, [is3D])

  const paintNode = useCallback((node: any, ctx: CanvasRenderingContext2D, globalScale: number) => {
    const r = NODE_SIZES[node.type] || 5
    const color = NODE_COLORS[node.type] || "#888"

    if (node.type === "user") {
      ctx.beginPath()
      ctx.arc(node.x, node.y, r + 4, 0, 2 * Math.PI)
      ctx.fillStyle = "rgba(139, 92, 246, 0.25)"
      ctx.fill()
    }


    // Halo for network root/source nodes (other uploaded networks)
    if (node.is_source && node.type === "person") {
      ctx.beginPath()
      ctx.arc(node.x, node.y, r + 6, 0, 2 * Math.PI)
      ctx.strokeStyle = "rgba(56, 189, 248, 0.7)" // cyan ring
      ctx.lineWidth = 1.5
      ctx.stroke()
    }

    ctx.beginPath()
    ctx.arc(node.x, node.y, r, 0, 2 * Math.PI)
    ctx.fillStyle = color
    ctx.fill()

    if (node.type === "company" && node.logo) {
      const logo = loadLogo(node.logo)
      if (logo) {
        ctx.save()
        ctx.beginPath()
        ctx.arc(node.x, node.y, r - 1, 0, 2 * Math.PI)
        ctx.clip()
        ctx.drawImage(logo, node.x - (r - 1), node.y - (r - 1), (r - 1) * 2, (r - 1) * 2)
        ctx.restore()
        ctx.beginPath()
        ctx.arc(node.x, node.y, r, 0, 2 * Math.PI)
        ctx.strokeStyle = "#F59E0B"
        ctx.lineWidth = 1
        ctx.stroke()
      }
    }

    if (node.type !== "company" && node.initials) {
      const initFontSize = Math.max(r * 1.1, 3)
      ctx.font = `${initFontSize}px Inter, sans-serif`
      ctx.textAlign = "center"
      ctx.textBaseline = "middle"
      ctx.fillStyle = "#FFFFFF"
      ctx.fillText(node.initials, node.x, node.y)
    }

    if (node.is_recruiter) {
      ctx.beginPath()
      ctx.arc(node.x, node.y, r + 2, 0, 2 * Math.PI)
      ctx.strokeStyle = "#10B981"
      ctx.lineWidth = 1.5
      ctx.stroke()
    }

    const fontSize = Math.max(10 / globalScale, 2)
    if (globalScale > 0.6 || node.type === "user" || node.type === "company") {
      ctx.font = `${node.type === "user" ? "bold " : ""}${fontSize}px Inter, sans-serif`
      ctx.textAlign = "center"
      ctx.textBaseline = "top"
      ctx.fillStyle = node.type === "company" ? "#FCD34D" : "#E4E4E7"
      const label = node.is_source && node.network_name
        ? `${node.name} · ${node.network_name}`
        : node.name || ""
      ctx.fillText(label, node.x, node.y + r + 2)
    }
  }, [])

  const paintLink = useCallback((link: any, ctx: CanvasRenderingContext2D) => {
    const src = link.source
    const tgt = link.target
    if (!src || !tgt || src.x == null || tgt.x == null) return

    ctx.beginPath()
    ctx.moveTo(src.x, src.y)
    ctx.lineTo(tgt.x, tgt.y)
    ctx.strokeStyle = link.label === "KNOWS" ? "rgba(139,92,246,0.3)" : "rgba(245,158,11,0.3)"
    ctx.lineWidth = link.label === "KNOWS" ? 0.8 : 0.5
    ctx.stroke()
  }, [])

  const create3DNode = useCallback((node: any) => {
    const size = NODE_SIZES_3D[node.type as string] || 4
    const color = NODE_COLORS[node.type as string] || "#888"
    
    const group = new THREE.Group()

    // Main shape
    const geometry = node.type === "company" 
      ? new THREE.BoxGeometry(size, size, size)
      : new THREE.SphereGeometry(size, 16, 16)
    
    const material = new THREE.MeshLambertMaterial({
      color: color,
      transparent: true,
      opacity: 0.9,
    })
    
    const mesh = new THREE.Mesh(geometry, material)
    group.add(mesh)
    
    // Glow for user node
    if (node.type === "user") {
      const glowGeometry = new THREE.SphereGeometry(size * 1.5, 16, 16)
      const glowMaterial = new THREE.MeshBasicMaterial({
        color: "#8B5CF6",
        transparent: true,
        opacity: 0.2,
      })
      const glow = new THREE.Mesh(glowGeometry, glowMaterial)
      group.add(glow)
    }
    
    // Recruiter ring
    if (node.is_recruiter) {
      const ringGeometry = new THREE.RingGeometry(size * 1.2, size * 1.4, 32)
      const ringMaterial = new THREE.MeshBasicMaterial({
        color: "#10B981",
        side: THREE.DoubleSide,
        transparent: true,
        opacity: 0.8,
      })
      const ring = new THREE.Mesh(ringGeometry, ringMaterial)
      group.add(ring)
    }

    // Initials sprite for person/user nodes
    if (node.type !== "company" && node.initials) {
      const canvas = document.createElement("canvas")
      canvas.width = 64
      canvas.height = 64
      const ctx2d = canvas.getContext("2d")!
      ctx2d.clearRect(0, 0, 64, 64)
      ctx2d.font = "bold 32px Inter, sans-serif"
      ctx2d.textAlign = "center"
      ctx2d.textBaseline = "middle"
      ctx2d.fillStyle = "#FFFFFF"
      ctx2d.fillText(node.initials, 32, 32)
      const tex = new THREE.CanvasTexture(canvas)
      const spriteMat = new THREE.SpriteMaterial({ map: tex, transparent: true, depthTest: false })
      const sprite = new THREE.Sprite(spriteMat)
      sprite.scale.set(size * 1.6, size * 1.6, 1)
      group.add(sprite)
    }

    // Company logo sprite (Sprites always face the camera automatically)
    if (node.type === "company" && node.logo) {
      const cached = loadLogo(node.logo)
      if (cached) {
        const tex = new THREE.Texture(cached)
        tex.needsUpdate = true
        const spriteMat = new THREE.SpriteMaterial({ map: tex, transparent: true, depthTest: false })
        const sprite = new THREE.Sprite(spriteMat)
        sprite.scale.set(size * 2, size * 2, 1)
        group.add(sprite)
      } else {
        // Image not loaded yet — try again when it loads
        const img = new Image()
        img.crossOrigin = "anonymous"
        img.onload = () => {
          logoCache.set(node.logo, img)
          const tex = new THREE.Texture(img)
          tex.needsUpdate = true
          const spriteMat = new THREE.SpriteMaterial({ map: tex, transparent: true, depthTest: false })
          const sprite = new THREE.Sprite(spriteMat)
          sprite.scale.set(size * 2, size * 2, 1)
          group.add(sprite)
        }
        img.src = node.logo
      }
    }
    
    return group
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="text-center">
          <div className="w-8 h-8 border-2 border-brand-500 border-t-transparent rounded-full animate-spin mx-auto mb-4" />
          <p className="text-zinc-400">Building your network graph...</p>
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex items-center justify-center h-full">
        <p className="text-accent-rose">{error}</p>
      </div>
    )
  }

  return (
    <div className="relative w-full h-full">
      {/* 2D/3D Toggle */}
      <div className="absolute top-4 right-4 z-10">
        <button
          onClick={() => {
            setIs3D(!is3D)
            setHasInitialZoomed(false)
          }}
          className="flex items-center gap-2 px-3 py-2 rounded-xl bg-dark-bg/90 backdrop-blur-md border border-dark-glassBorder text-sm text-zinc-300 hover:text-white hover:border-brand-500 transition-all"
          title={is3D ? "Switch to 2D" : "Switch to 3D"}
        >
          {is3D ? <Square className="w-4 h-4" /> : <Box className="w-4 h-4" />}
          <span>{is3D ? "2D" : "3D"}</span>
        </button>
      </div>

      {/* Search Bar */}
      <div className="absolute top-4 left-1/2 -translate-x-1/2 z-10 w-80">
        <div className="relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-zinc-500" />
          <input
            type="text"
            value={searchQuery}
            onChange={e => setSearchQuery(e.target.value)}
            onFocus={() => setIsFocused(true)}
            onBlur={() => setTimeout(() => setIsFocused(false), 150)}
            placeholder="Search companies or people..."
            className="w-full pl-10 pr-10 py-2.5 rounded-xl bg-dark-bg/90 backdrop-blur-md border border-dark-glassBorder text-sm text-white placeholder-zinc-500 focus:outline-none focus:border-brand-500 focus:ring-1 focus:ring-brand-500/30 transition-all"
          />
          {searchQuery && (
            <button
              onClick={() => setSearchQuery("")}
              className="absolute right-3 top-1/2 -translate-y-1/2 text-zinc-500 hover:text-white transition-colors"
            >
              <X className="w-4 h-4" />
            </button>
          )}
        </div>

        {isFocused && suggestions.length > 0 && (
          <div className="mt-1 rounded-xl bg-dark-bg/95 backdrop-blur-md border border-dark-glassBorder overflow-hidden shadow-lg max-h-64 overflow-y-auto">
            {suggestions.map(item => (
              <button
                key={`${item.type}-${item.name}`}
                onMouseDown={() => {
                  setSearchQuery(item.name)
                  setIsFocused(false)
                }}
                className="w-full text-left px-4 py-2 text-sm text-zinc-300 hover:bg-brand-500/20 hover:text-white transition-colors flex items-center gap-2"
              >
                <div className={`w-2 h-2 rounded-full ${item.type === "company" ? "bg-[#F59E0B]" : "bg-[#06B6D4]"}`} />
                <span className="truncate">{item.name}</span>
                <span className="text-[10px] text-zinc-600 ml-auto shrink-0">{item.type === "company" ? "Company" : "Person"}</span>
              </button>
            ))}
          </div>
        )}

        {searchQuery && filteredData !== data && (
          <div className="mt-2 flex items-center justify-center gap-2">
            <span className="text-xs text-zinc-400 bg-dark-bg/80 backdrop-blur-sm px-3 py-1 rounded-full border border-dark-glassBorder">
              Showing <span className="text-brand-400 font-medium">{filteredData.nodes.length}</span> nodes
              {" · "}
              <button onClick={() => setSearchQuery("")} className="text-accent-cyan hover:text-white transition-colors">
                Clear filter
              </button>
            </span>
          </div>
        )}
      </div>

      {/* Person Detail Panel */}
      {selectedNode && (
        <div className="absolute top-4 right-20 z-10 w-72 rounded-2xl bg-dark-bg/95 backdrop-blur-md border border-dark-glassBorder shadow-lg overflow-hidden">
          <div className="relative px-5 pt-5 pb-4 border-b border-dark-glassBorder">
            <button
              onClick={() => setSelectedNode(null)}
              className="absolute top-3 right-3 text-zinc-500 hover:text-white transition-colors"
            >
              <X className="w-4 h-4" />
            </button>
            <div className="flex items-center gap-3">
              <div
                className={`w-10 h-10 rounded-full flex items-center justify-center text-sm font-semibold text-white ${
                  selectedNode.type === "user" ? "bg-[#8B5CF6]" : "bg-[#06B6D4]"
                }`}
              >
                {selectedNode.initials || selectedNode.name?.charAt(0) || "?"}
              </div>
              <div className="min-w-0">
                <h3 className="text-sm font-semibold text-white truncate">{selectedNode.name}</h3>
                {selectedNode.type === "user" && (
                  <span className="text-xs text-brand-400 font-medium">You</span>
                )}
              </div>
            </div>
          </div>

          <div className="px-5 py-4 space-y-3">
            {selectedNode.title && (
              <div className="flex items-start gap-3">
                <Briefcase className="w-4 h-4 text-zinc-500 mt-0.5 shrink-0" />
                <div>
                  <p className="text-xs text-zinc-500">Position</p>
                  <p className="text-sm text-zinc-200">{selectedNode.title}</p>
                </div>
              </div>
            )}

            <div className="flex items-start gap-3">
              <User className="w-4 h-4 text-zinc-500 mt-0.5 shrink-0" />
              <div>
                <p className="text-xs text-zinc-500">Type</p>
                <p className="text-sm text-zinc-200 capitalize">{selectedNode.type === "user" ? "You" : "1st Connection"}</p>
              </div>
            </div>

            {selectedNode.is_recruiter && (
              <div className="flex items-start gap-3">
                <Shield className="w-4 h-4 text-emerald-500 mt-0.5 shrink-0" />
                <div>
                  <p className="text-xs text-zinc-500">Recruiter</p>
                  <p className="text-sm text-emerald-400 font-medium">Yes</p>
                </div>
              </div>
            )}

            {selectedNode.connected_on && (
              <div className="flex items-start gap-3">
                <Calendar className="w-4 h-4 text-zinc-500 mt-0.5 shrink-0" />
                <div>
                  <p className="text-xs text-zinc-500">Connected On</p>
                  <p className="text-sm text-zinc-200">{selectedNode.connected_on}</p>
                </div>
              </div>
            )}
          </div>

          {selectedNode.type === "person" && (
            <div className="px-5 pb-4 space-y-2">
              <button
                onClick={() => {
                  // Find the company this person works at via WORKS_AT links
                  const worksAtLink = data.links.find(link => {
                    const srcId = typeof link.source === "string" ? link.source : link.source.id
                    return srcId === selectedNode.id && link.label === "WORKS_AT"
                  })
                  const companyId = worksAtLink
                    ? (typeof worksAtLink.target === "string" ? worksAtLink.target : worksAtLink.target.id)
                    : ""
                  const companyNode = companyId ? data.nodes.find(n => n.id === companyId) : null
                  const companyName = companyNode?.name || ""
                  router.push(`/search?q=${encodeURIComponent(companyName)}&person=${encodeURIComponent(selectedNode.name)}`)
                }}
                className="flex items-center justify-center gap-2 w-full py-2.5 rounded-xl bg-brand-600 hover:bg-brand-500 text-white text-sm font-medium transition-colors"
              >
                <MessageSquare className="w-4 h-4" />
                Draft Message
              </button>
              {selectedNode.profile_url && (
                <a
                  href={selectedNode.profile_url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="flex items-center justify-center gap-2 w-full py-2.5 rounded-xl bg-[#0A66C2] hover:bg-[#0A66C2]/80 text-white text-sm font-medium transition-colors"
                >
                  <ExternalLink className="w-4 h-4" />
                  View LinkedIn Profile
                </a>
              )}
            </div>
          )}
        </div>
      )}

      {/* 3D Mode Instructions */}
      {is3D && (
        <div className="absolute bottom-4 left-4 z-10 px-4 py-2 rounded-xl bg-dark-bg/80 backdrop-blur-sm border border-dark-glassBorder">
          <p className="text-xs text-zinc-500">
            <span className="text-zinc-300">Drag</span> to rotate · <span className="text-zinc-300">Scroll</span> to zoom · <span className="text-zinc-300">Right-drag</span> to pan
          </p>
        </div>
      )}

      {is3D ? (
        <ForceGraph3D
          ref={fgRef}
          graphData={filteredData}
          width={width}
          height={height}
          backgroundColor="rgba(10,10,18,0)"
          nodeThreeObject={create3DNode}
          nodeLabel={(node: any) => {
            const label = `${node.name}${node.title ? ` - ${node.title}` : ''}`
            return node.type === "company" && node.url ? `${label} (click to visit)` : label
          }}
          linkColor={(link: any) => link.label === "KNOWS" ? "rgba(139,92,246,0.4)" : "rgba(245,158,11,0.4)"}
          linkWidth={1}
          linkOpacity={0.6}
          onNodeClick={handleNodeClick}
          onNodeDragEnd={handleNodeDragEnd}
          onBackgroundClick={() => setSelectedNode(null)}
          onEngineStop={handleEngineStop}
          cooldownTicks={100}
          enableNodeDrag={true}
          enableNavigationControls={true}
          showNavInfo={false}
        />
      ) : (
        <ForceGraph2D
          ref={fgRef}
          graphData={filteredData}
          width={width}
          height={height}
          backgroundColor="rgba(0,0,0,0)"
          nodeCanvasObject={paintNode}
          nodePointerAreaPaint={(node: any, color: string, ctx: CanvasRenderingContext2D) => {
            const r = NODE_SIZES[node.type as string] || 5
            ctx.beginPath()
            ctx.arc(node.x, node.y, r + 4, 0, 2 * Math.PI)
            ctx.fillStyle = color
            ctx.fill()
          }}
          linkCanvasObject={paintLink}
          linkDirectionalArrowLength={4}
          linkDirectionalArrowRelPos={0.9}
          onNodeClick={handleNodeClick}
          onNodeDragEnd={handleNodeDragEnd}
          onBackgroundClick={() => setSelectedNode(null)}
          onEngineStop={handleEngineStop}
          cooldownTicks={100}
          d3AlphaDecay={0.02}
          d3VelocityDecay={0.3}
        />
      )}
    </div>
  )
}
