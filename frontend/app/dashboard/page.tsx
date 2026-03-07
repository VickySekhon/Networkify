"use client"
import { useState, useEffect } from "react"
import Link from "next/link"
import { useRouter, usePathname } from "next/navigation"
import axios from "axios"
import {
  Waypoints, Home, Users, Upload, Settings, Search,
  ArrowUpRight, Zap, Droplets, ChevronLeft, ChevronRight
} from "lucide-react"
import Sidebar from "@/components/Sidebar"

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000"

interface MetricBarProps {
  icon: React.ReactNode
  label: string
  value: string
  target: string
  percentage: number
  color: string
}

function MetricBar({ icon, label, value, target, percentage, color }: MetricBarProps) {
  return (
    <div className="bg-dark-surface rounded-2xl p-4">
      <div className="flex items-center gap-3 mb-3">
        <div className={`w-10 h-10 rounded-xl ${color} flex items-center justify-center`}>
          {icon}
        </div>
        <div className="flex-1">
          <p className="text-sm text-zinc-400">{label}</p>
          <p className="text-white font-semibold">{value} <span className="text-zinc-500 font-normal">/ {target}</span></p>
        </div>
        <span className="text-sm text-zinc-400">{percentage}%</span>
      </div>
      <div className="h-1.5 bg-dark-elevated rounded-full overflow-hidden">
        <div
          className={`h-full rounded-full transition-all duration-500 ${color.replace('bg-', 'bg-').replace('/20', '')}`}
          style={{ width: `${percentage}%` }}
        />
      </div>
    </div>
  )
}

interface StatCardProps {
  icon: React.ReactNode
  value: string
  label: string
  color: string
}

function StatCard({ icon, value, label, color }: StatCardProps) {
  return (
    <div className="bg-dark-surface rounded-2xl p-5 flex flex-col items-center justify-center text-center min-h-[120px]">
      <div className={`w-10 h-10 rounded-xl ${color} flex items-center justify-center mb-3`}>
        {icon}
      </div>
      <p className="text-2xl font-bold text-white mb-1">{value}</p>
      <p className="text-xs text-zinc-500">{label}</p>
    </div>
  )
}

interface WeeklyBarProps {
  day: string
  height: number
  isActive?: boolean
}

function WeeklyBar({ day, height, isActive }: WeeklyBarProps) {
  return (
    <div className="flex flex-col items-center gap-2 flex-1">
      <div className="w-full h-24 bg-dark-elevated rounded-lg relative overflow-hidden">
        <div
          className={`absolute bottom-0 left-0 right-0 rounded-lg transition-all duration-500 ${isActive ? 'bg-gradient-to-t from-accent-rose to-accent-amber' : 'bg-gradient-to-t from-accent-rose/60 to-accent-amber/60'}`}
          style={{ height: `${height}%` }}
        />
      </div>
      <span className="text-xs text-zinc-500">{day}</span>
    </div>
  )
}


export default function Dashboard() {
  const router = useRouter()
  const [currentMonth] = useState(new Date())
  const [stats, setStats] = useState({ connections: 0, companies: 0, recruiters: 0, top_companies: [] as any[] })
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    const fetchStats = async () => {
      const userId = localStorage.getItem("user_id") || ""
      if (!userId) {
        setLoading(false)
        return
      }
      try {
        const res = await axios.get(`${API_URL}/api/graph/stats`, {
          params: { user_id: userId }
        })
        setStats(res.data)
      } catch (e) {
        console.error("Failed to fetch stats:", e)
      } finally {
        setLoading(false)
      }
    }
    fetchStats()
  }, [])

  const weeklyData = [
    { day: "Sun", height: 40 },
    { day: "Mon", height: 65 },
    { day: "Tue", height: 45 },
    { day: "Wed", height: 80 },
    { day: "Thu", height: 55 },
    { day: "Fri", height: 90, isActive: true },
    { day: "Sat", height: 35 },
  ]

  const metrics = {
    connections: { value: String(stats.connections), target: "500", percentage: Math.min(100, Math.round((stats.connections / 500) * 100)) },
    companies: { value: String(stats.companies), target: "100", percentage: Math.min(100, Math.round((stats.companies / 100) * 100)) },
    paths: { value: String(stats.recruiters), target: "50", percentage: Math.min(100, Math.round((stats.recruiters / 50) * 100)) },
  }

  const calendarDays = Array.from({ length: 35 }, (_, i) => {
    const day = i - 3
    return day > 0 && day <= 31 ? day : null
  })

  const activityDays = [3, 4, 5, 10, 11, 12, 17]

  return (
    <div className="flex h-screen bg-dark-bg overflow-hidden">
      <Sidebar />

      <main className="flex-1 p-6 overflow-auto">
        <div className="max-w-7xl mx-auto grid grid-cols-12 gap-4 auto-rows-min">

          <div className="col-span-12 lg:col-span-5 bg-dark-surface rounded-3xl p-6">
            <div className="flex items-center justify-between mb-6">
              <h2 className="text-xl font-semibold text-white">Network Activity</h2>
              <button className="w-8 h-8 rounded-lg bg-dark-elevated flex items-center justify-center text-zinc-400 hover:text-white transition-colors">
                <ArrowUpRight className="w-4 h-4" />
              </button>
            </div>

            <div className="flex items-center justify-between mb-4">
              <button className="text-zinc-500 hover:text-white transition-colors">
                <ChevronLeft className="w-5 h-5" />
              </button>
              <div className="flex gap-8 text-sm">
                {["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"].map((d) => (
                  <span key={d} className="text-zinc-500 w-8 text-center">{d}</span>
                ))}
              </div>
              <button className="text-zinc-500 hover:text-white transition-colors">
                <ChevronRight className="w-5 h-5" />
              </button>
            </div>

            <div className="grid grid-cols-7 gap-2 mb-6">
              {calendarDays.map((day, i) => {
                const hasActivity = day && activityDays.includes(day)
                const isToday = day === 17
                return (
                  <div
                    key={i}
                    className={`aspect-square rounded-xl flex items-center justify-center text-sm relative ${day === null
                      ? "text-zinc-700"
                      : isToday
                        ? "bg-brand-500 text-white font-medium"
                        : "text-zinc-400 hover:bg-dark-elevated transition-colors cursor-pointer"
                      }`}
                  >
                    {day}
                    {hasActivity && !isToday && (
                      <div className="absolute bottom-1 left-1/2 -translate-x-1/2 flex gap-0.5">
                        <div className="w-1 h-1 rounded-full bg-accent-rose" />
                        <div className="w-1 h-1 rounded-full bg-accent-rose" />
                      </div>
                    )}
                  </div>
                )
              })}
            </div>

            <div className="grid grid-cols-3 gap-4 pt-4 border-t border-dark-glassBorder">
              <div>
                <p className="text-xs text-zinc-500 mb-1">Connections</p>
                <p className="text-lg font-semibold text-white">{loading ? "..." : stats.connections}</p>
              </div>
              <div>
                <p className="text-xs text-zinc-500 mb-1">Companies</p>
                <p className="text-lg font-semibold text-white">{loading ? "..." : stats.companies}</p>
              </div>
              <div>
                <p className="text-xs text-zinc-500 mb-1">Recruiters</p>
                <p className="text-lg font-semibold text-white">{loading ? "..." : stats.recruiters}</p>
              </div>
            </div>
          </div>

          <div className="col-span-12 lg:col-span-7 bg-gradient-to-br from-brand-600/40 via-brand-500/30 to-accent-cyan/20 rounded-3xl p-8 relative overflow-hidden min-h-[360px]">
            <div className="relative z-10">
              <h1 className="text-4xl lg:text-5xl font-bold text-white leading-tight mb-4">
                Your Network,<br />
                Your Power.
              </h1>
              <p className="text-zinc-300 max-w-sm mb-6">
                Discover the warmest paths to any company with clarity and purpose.
              </p>
              <div className="inline-flex items-center gap-2 px-4 py-2 rounded-full bg-white/10 backdrop-blur-sm border border-white/20">
                <Zap className="w-4 h-4 text-accent-amber" />
                <span className="text-sm text-white">AI-Powered Path Discovery</span>
              </div>
            </div>

            <div className="absolute right-0 top-0 bottom-0 w-1/2 flex items-center justify-center">
              <div className="relative w-48 h-48">
                <div className="absolute inset-0 rounded-full border border-white/10 animate-pulse-slow" />
                <div className="absolute inset-6 rounded-full border border-white/10" />
                <div className="absolute inset-12 rounded-full border border-white/20" />
                <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-16 h-16 rounded-full bg-gradient-to-br from-brand-400 to-accent-cyan flex items-center justify-center shadow-glow">
                  <Waypoints className="w-8 h-8 text-white" />
                </div>
                {[
                  { top: "5%", left: "50%", color: "bg-accent-cyan" },
                  { top: "25%", left: "90%", color: "bg-purple-400" },
                  { top: "75%", left: "85%", color: "bg-accent-emerald" },
                  { top: "90%", left: "50%", color: "bg-accent-amber" },
                  { top: "70%", left: "10%", color: "bg-accent-rose" },
                  { top: "20%", left: "15%", color: "bg-blue-400" },
                ].map((node, i) => (
                  <div
                    key={i}
                    className={`absolute w-3 h-3 rounded-full ${node.color} shadow-lg animate-float`}
                    style={{
                      top: node.top,
                      left: node.left,
                      transform: "translate(-50%, -50%)",
                      animationDelay: `${i * 0.4}s`
                    }}
                  />
                ))}
              </div>
            </div>

            <div className="absolute bottom-6 right-6 flex gap-2">
              <div className="w-2 h-2 rounded-full bg-white" />
              <div className="w-2 h-2 rounded-full bg-white/30" />
              <div className="w-2 h-2 rounded-full bg-white/30" />
            </div>
          </div>

          <div className="col-span-12 lg:col-span-5 space-y-4">
            <MetricBar
              icon={<Droplets className="w-5 h-5 text-accent-cyan" />}
              label="Connections"
              value={metrics.connections.value}
              target={metrics.connections.target}
              percentage={metrics.connections.percentage}
              color="bg-accent-cyan/20"
            />
            <MetricBar
              icon={<Zap className="w-5 h-5 text-accent-amber" />}
              label="Companies"
              value={metrics.companies.value}
              target={metrics.companies.target}
              percentage={metrics.companies.percentage}
              color="bg-accent-amber/20"
            />
            <MetricBar
              icon={<Waypoints className="w-5 h-5 text-accent-emerald" />}
              label="Referral Paths"
              value={metrics.paths.value}
              target={metrics.paths.target}
              percentage={metrics.paths.percentage}
              color="bg-accent-emerald/20"
            />
          </div>

          <div className="col-span-12 lg:col-span-4 bg-dark-surface rounded-3xl p-6">
            <div className="flex items-center justify-between mb-6">
              <h3 className="text-lg font-semibold text-white">This week</h3>
              <div className="flex items-center gap-2">
                <div className="w-2 h-2 rounded-full bg-accent-rose" />
                <span className="text-xs text-zinc-500">Last week</span>
              </div>
            </div>

            <div className="flex gap-2">
              {weeklyData.map((bar, i) => (
                <WeeklyBar key={i} {...bar} />
              ))}
            </div>
          </div>

          <div className="col-span-6 lg:col-span-3 grid grid-cols-1 gap-4">
            <StatCard
              icon={<Zap className="w-5 h-5 text-accent-amber" />}
              value={loading ? "..." : String(stats.recruiters)}
              label="Recruiters Found"
              color="bg-accent-amber/20"
            />
          </div>
        </div>
      </main>
    </div>
  )
}
