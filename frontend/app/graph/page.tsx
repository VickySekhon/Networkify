"use client"
import { useState, useEffect } from "react"
import { useRouter } from "next/navigation"
import Link from "next/link"
import { Waypoints, ArrowLeft, ZoomIn, ZoomOut, Maximize2 } from "lucide-react"
import Sidebar from "@/components/Sidebar"
import Graph from "@/components/graph/Graph"

export default function GraphPage() {
  const router = useRouter()
  const [dimensions, setDimensions] = useState({ width: 800, height: 600 })

  useEffect(() => {
    function handleResize() {
      // Subtract sidebar width (80px) and some padding
      setDimensions({
        width: window.innerWidth - 80 - 48,
        height: window.innerHeight - 140,
      })
    }
    handleResize()
    window.addEventListener("resize", handleResize)
    return () => window.removeEventListener("resize", handleResize)
  }, [])

  return (
    <div className="flex h-screen bg-dark-bg overflow-hidden">
      <Sidebar />

      <main className="flex-1 p-6 flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between mb-6">
          <div className="flex items-center gap-4">
            <button
              onClick={() => router.back()}
              className="w-10 h-10 rounded-xl bg-dark-surface border border-dark-glassBorder flex items-center justify-center text-zinc-400 hover:text-white transition-colors"
            >
              <ArrowLeft className="w-5 h-5" />
            </button>
            <div>
              <h1 className="text-2xl font-bold text-white">Network Graph</h1>
              <p className="text-sm text-zinc-500">
                Your LinkedIn network visualized
              </p>
            </div>
          </div>

          {/* Legend */}
          <div className="flex items-center gap-6 px-4 py-2 rounded-xl bg-dark-surface border border-dark-glassBorder">
            <div className="flex items-center gap-2">
              <div className="w-3 h-3 rounded-full bg-[#8B5CF6]" />
              <span className="text-xs text-zinc-400">You</span>
            </div>
            <div className="flex items-center gap-2">
              <div className="w-3 h-3 rounded-full bg-[#06B6D4]" />
              <span className="text-xs text-zinc-400">Connection</span>
            </div>
            <div className="flex items-center gap-2">
              <div className="w-3 h-3 rounded-full bg-[#F59E0B]" />
              <span className="text-xs text-zinc-400">Company</span>
            </div>
            <div className="flex items-center gap-2">
              <div className="w-3 h-3 rounded-full border-2 border-[#10B981] bg-transparent" />
              <span className="text-xs text-zinc-400">Recruiter</span>
            </div>
          </div>
        </div>

        {/* Graph Container */}
        <div className="flex-1 rounded-2xl bg-dark-surface border border-dark-glassBorder overflow-hidden relative">
          <Graph width={dimensions.width} height={dimensions.height} />

          {/* Edge labels info */}
          <div className="absolute bottom-4 left-4 flex gap-4 px-4 py-2 rounded-xl bg-dark-bg/80 backdrop-blur-sm border border-dark-glassBorder">
            <div className="flex items-center gap-2">
              <div className="w-6 h-0.5 bg-[#8B5CF6]/60" />
              <span className="text-xs text-zinc-500">knows</span>
            </div>
            <div className="flex items-center gap-2">
              <div className="w-6 h-0.5 bg-[#F59E0B]/60" />
              {/* <span className="text-xs text-zinc-500">works at</span> */}
            </div>
          </div>
        </div>
      </main>
    </div>
  )
}
