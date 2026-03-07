import Link from "next/link"
import { ArrowRight, Waypoints, Network, TrendingUp, Users, Zap } from "lucide-react"

export default function Home() {
  return (
    <div className="min-h-screen bg-[#0a0a12] relative overflow-hidden">
      {/* Cosmic background */}
      <div className="fixed inset-0 -z-10">
        {/* Stars/particles effect */}
        <div className="absolute inset-0 bg-[radial-gradient(ellipse_at_center,_transparent_0%,_#0a0a12_70%)]" />
        
        {/* Main glow ring */}
        <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[600px] h-[600px]">
          <div className="absolute inset-0 rounded-full bg-gradient-to-t from-brand-600/40 via-purple-500/20 to-transparent blur-[80px] animate-pulse-slow" />
          <div className="absolute inset-12 rounded-full border border-purple-500/20 animate-[spin_60s_linear_infinite]" />
          <div className="absolute inset-24 rounded-full border border-brand-500/10 animate-[spin_45s_linear_infinite_reverse]" />
        </div>

        {/* Video placeholder - replace with your 3D graph video */}
        <div className="absolute inset-0 flex items-center justify-center">
          {/* 
            To add your video:
            <video autoPlay muted loop playsInline className="w-full h-full object-cover opacity-60">
              <source src="/your-video.mp4" type="video/mp4" />
            </video>
          */}
        </div>

        {/* Particle dots */}
        {[...Array(50)].map((_, i) => (
          <div
            key={i}
            className="absolute w-0.5 h-0.5 bg-white/30 rounded-full animate-pulse"
            style={{
              top: `${Math.random() * 100}%`,
              left: `${Math.random() * 100}%`,
              animationDelay: `${Math.random() * 3}s`,
              animationDuration: `${2 + Math.random() * 3}s`,
            }}
          />
        ))}
      </div>

      {/* Navigation */}
      <nav className="relative z-50 px-8 py-6">
        <div className="max-w-7xl mx-auto flex items-center justify-between">
          <div className="text-xs tracking-[0.2em] text-zinc-500 uppercase">
            Network Intelligence
          </div>
          
          <div className="flex items-center gap-8 text-sm">
            <Link href="/dashboard" className="text-zinc-400 hover:text-white transition-colors tracking-wide">
              Terminal
            </Link>
            <Link href="/connections" className="text-zinc-400 hover:text-white transition-colors tracking-wide">
              Connections
            </Link>
            <Link href="/search" className="text-zinc-400 hover:text-white transition-colors tracking-wide">
              Analytics
            </Link>
            <span className="text-zinc-600">/</span>
            <Link href="#" className="text-zinc-400 hover:text-white transition-colors tracking-wide">
              Docs
            </Link>
          </div>

          <Link 
            href="/login" 
            className="px-5 py-2 rounded-full border border-zinc-700 text-sm text-white hover:bg-white/5 transition-all flex items-center gap-2"
          >
            Launch App
            <ArrowRight className="w-4 h-4" />
          </Link>
        </div>
      </nav>

      {/* Hero Section */}
      <main className="relative z-10 flex flex-col items-center justify-center min-h-[calc(100vh-200px)] px-6">
        <div className="text-center max-w-4xl mx-auto">
          {/* Small label */}
          <div className="inline-flex items-center gap-2 text-xs tracking-[0.15em] text-purple-400/80 uppercase mb-8">
            <Waypoints className="w-3.5 h-3.5" />
            <span>Pathfinder</span>
          </div>

          {/* Main heading */}
          <h1 className="text-5xl sm:text-6xl lg:text-7xl font-light text-white leading-[1.1] mb-8 tracking-tight">
            <span className="text-[#c8b6ff]">AI-Driven</span>
            <br />
            <span className="font-normal">Network</span>
            <br />
            <span className="font-normal">Intelligence</span>
          </h1>

          {/* Subtitle */}
          <p className="text-zinc-500 max-w-md mx-auto mb-12 leading-relaxed">
            Discover the warmest paths to any company with clarity and purpose.
          </p>

          {/* Stats card */}
          <div className="inline-flex items-center gap-6 bg-[#12121a]/80 backdrop-blur-xl border border-zinc-800/50 rounded-2xl px-6 py-4 mb-12">
            <div className="flex items-center gap-3">
              <div className="w-8 h-8 rounded-lg bg-purple-500/10 flex items-center justify-center">
                <Network className="w-4 h-4 text-purple-400" />
              </div>
              <div className="text-left">
                <p className="text-[10px] text-zinc-500 uppercase tracking-wider">Paths Found</p>
                <p className="text-white font-medium">2.4k+</p>
              </div>
            </div>
            
            <div className="w-px h-8 bg-zinc-800" />
            
            <div className="flex items-center gap-3">
              <TrendingUp className="w-4 h-4 text-zinc-600" />
              <div className="flex items-center gap-2">
                <span className="text-2xl font-semibold text-white">$1.9B+</span>
                <div className="flex gap-0.5">
                  {[...Array(6)].map((_, i) => (
                    <div key={i} className={`w-1 rounded-full ${i < 4 ? 'h-4 bg-purple-400' : 'h-2 bg-zinc-700'}`} />
                  ))}
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Side labels */}
        <div className="absolute left-8 bottom-32 max-w-[200px]">
          <p className="text-[10px] text-zinc-600 uppercase tracking-wider mb-2">Career_Intelligence</p>
          <h3 className="text-lg font-medium text-white mb-4 leading-snug">
            Autonomous networking reinvented
          </h3>
          <Link 
            href="/dashboard" 
            className="inline-flex items-center gap-2 px-4 py-2 rounded-full border border-zinc-800 text-xs text-zinc-400 hover:text-white hover:border-zinc-600 transition-all"
          >
            <Network className="w-3.5 h-3.5" />
            Explore
          </Link>
        </div>

        <div className="absolute right-8 bottom-32 max-w-[250px] text-right">
          <p className="text-[10px] text-zinc-500 leading-relaxed">
            Pathfinder is a next-generation networking engine designed to navigate the complexity of professional relationships.
          </p>
        </div>

        {/* Bottom CTA */}
        <div className="absolute bottom-8 left-1/2 -translate-x-1/2 flex items-center gap-4">
          <div className="flex items-center gap-2 text-sm text-zinc-500">
            <div className="w-6 h-6 rounded-full border border-zinc-700 flex items-center justify-center">
              <Waypoints className="w-3 h-3" />
            </div>
            <span>UNIV.0</span>
            <span className="text-zinc-700">=</span>
          </div>
          
          <Link 
            href="/login" 
            className="px-5 py-2.5 rounded-full bg-[#d4ff00] text-black text-sm font-medium hover:bg-[#e5ff4d] transition-all flex items-center gap-2"
          >
            <Zap className="w-4 h-4" />
            Connect
          </Link>
        </div>
      </main>

      {/* Bottom tech icons */}
      <div className="relative z-10 border-t border-zinc-800/50 py-6">
        <div className="max-w-4xl mx-auto flex items-center justify-center gap-12">
          {[
            { icon: Network, label: "Neo4j" },
            { icon: Zap, label: "Gemini" },
            { icon: TrendingUp, label: "FastAPI" },
            { icon: Users, label: "LinkedIn" },
            { icon: Waypoints, label: "Cloudinary" },
          ].map((tech, i) => (
            <div key={i} className="flex flex-col items-center gap-2 text-zinc-600 hover:text-zinc-400 transition-colors cursor-pointer">
              <tech.icon className="w-5 h-5" />
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
