"use client"
import { useState } from "react"
import Link from "next/link"
import { useRouter } from "next/navigation"
import { Waypoints, Mail, Lock, Loader2, AlertCircle } from "lucide-react"
import axios from "axios"
import { useAuth } from "@/components/AuthContext"

export default function LoginPage() {
  const apiUrl = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000"
  const router = useRouter()
  const { login } = useAuth()
  
  const [email, setEmail] = useState("")
  const [password, setPassword] = useState("")
  const [name, setName] = useState("")
  const [isSignUp, setIsSignUp] = useState(false)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!email || !password || (isSignUp && !name)) return
    
    setLoading(true)
    setError(null)
    
    try {
      const endpoint = isSignUp ? "/auth/signup" : "/auth/login/password"
      const payload = isSignUp ? { email, password, name } : { email, password }
      
      const res = await axios.post(`${apiUrl}${endpoint}`, payload)
      
      const { token, user } = res.data
      login(token, user)
      router.push("/upload")
    } catch (err: any) {
      const rawError = err.response?.data?.detail || err.message || "An unexpected error occurred"
      
      if (typeof rawError === "object" && rawError.rules) {
        // Format password policy rules
        const unverified = rawError.rules
          .filter((r: any) => !r.verified)
          .map((r: any) => {
            let msg = r.message.replace("%d", r.format?.[0] || "")
            if (r.items) {
              const subItems = r.items
                .filter((si: any) => !si.verified)
                .map((si: any) => si.message)
                .join(", ")
              if (subItems) msg += " " + subItems
            }
            return msg
          })
        
        setError(`Password requirements: ${unverified.join("; ")}`)
      } else {
        let finalError = typeof rawError === "object" ? JSON.stringify(rawError) : rawError
        
        // Translate generic Auth0 error
        if (finalError === "Invalid sign up") {
          finalError = "Invalid sign up. This usually means the email is already registered or the password is too weak. Try logging in instead or use a complex password."
        }
        
        setError(finalError)
      }
      console.error("Auth Error:", rawError)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="min-h-screen bg-[#0a0a12] relative overflow-hidden flex items-center justify-center">
      {/* Cosmic background */}
      <div className="fixed inset-0 -z-10">
        <div className="absolute inset-0 bg-[radial-gradient(ellipse_at_center,_transparent_0%,_#0a0a12_70%)]" />

        {/* Main glow ring */}
        <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[500px] h-[500px]">
          <div className="absolute inset-0 rounded-full bg-gradient-to-t from-brand-600/40 via-purple-500/20 to-transparent blur-[80px] animate-pulse-slow" />
          <div className="absolute inset-12 rounded-full border border-purple-500/20 animate-[spin_60s_linear_infinite]" />
          <div className="absolute inset-24 rounded-full border border-brand-500/10 animate-[spin_45s_linear_infinite_reverse]" />
        </div>

        {/* Particle dots */}
        {[...Array(30)].map((_, i) => (
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

      <div className="relative z-10 w-full max-w-md px-6">
        {/* Logo / Brand */}
        <div className="text-center mb-10">
          <Link href="/" className="inline-flex items-center gap-3 mb-8 group">
            <div className="w-12 h-12 rounded-2xl bg-gradient-to-br from-brand-500 to-accent-cyan flex items-center justify-center shadow-glow group-hover:shadow-glow-lg transition-all">
              <Waypoints className="w-6 h-6 text-white" />
            </div>
            <span className="text-xl font-semibold text-white tracking-tight">Pathfinder</span>
          </Link>

          <h1 className="text-3xl font-bold text-white mb-3">
            {isSignUp ? "Create an account" : "Welcome back"}
          </h1>
          <p className="text-zinc-500 text-sm">
            {isSignUp ? "Join Pathfinder to map your professional growth" : "Sign in to explore your professional network"}
          </p>
        </div>

        {/* Login/Signup Card */}
        <div className="bg-[#12121a]/80 backdrop-blur-xl border border-zinc-800/50 rounded-2xl p-8">
          <form onSubmit={handleSubmit} className="space-y-4 mb-6">
            {isSignUp && (
              <div className="animate-slide-down">
                <label className="block text-sm font-medium text-zinc-400 mb-1.5 ml-1">Full Name</label>
                <div className="relative">
                  <Waypoints className="absolute left-4 top-1/2 -translate-y-1/2 w-4 h-4 text-zinc-500" />
                  <input
                    type="text"
                    value={name}
                    onChange={(e) => setName(e.target.value)}
                    placeholder="John Doe"
                    className="w-full bg-dark-bg/50 border border-zinc-800 rounded-xl py-3 pl-11 pr-4 text-white placeholder-zinc-600 focus:outline-none focus:border-brand-500 transition-all font-light"
                    required
                  />
                </div>
              </div>
            )}

            <div>
              <label className="block text-sm font-medium text-zinc-400 mb-1.5 ml-1">Email Address</label>
              <div className="relative">
                <Mail className="absolute left-4 top-1/2 -translate-y-1/2 w-4 h-4 text-zinc-500" />
                <input
                  type="email"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  placeholder="name@company.com"
                  className="w-full bg-dark-bg/50 border border-zinc-800 rounded-xl py-3 pl-11 pr-4 text-white placeholder-zinc-600 focus:outline-none focus:border-brand-500 transition-all font-light"
                  required
                />
              </div>
            </div>

            <div>
              <label className="block text-sm font-medium text-zinc-400 mb-1.5 ml-1">Password</label>
              <div className="relative">
                <Lock className="absolute left-4 top-1/2 -translate-y-1/2 w-4 h-4 text-zinc-500" />
                <input
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  placeholder="••••••••"
                  className="w-full bg-dark-bg/50 border border-zinc-800 rounded-xl py-3 pl-11 pr-4 text-white placeholder-zinc-600 focus:outline-none focus:border-brand-500 transition-all font-light"
                  required
                />
              </div>
            </div>

            {error && (
              <div className="flex flex-col gap-2 text-red-400 text-sm mt-2 bg-red-500/10 p-4 rounded-xl border border-red-500/20 animate-shake">
                <div className="flex items-center gap-2 font-semibold">
                  <AlertCircle className="w-5 h-5 shrink-0" />
                  <span>Authentication Error</span>
                </div>
                <p className="text-zinc-300 text-xs leading-relaxed pl-7">
                  {error}
                </p>
              </div>
            )}

            <button
              type="submit"
              disabled={loading}
              className="w-full btn-primary py-3.5 rounded-xl flex items-center justify-center gap-2 mt-2"
            >
              {loading ? <Loader2 className="w-5 h-5 animate-spin" /> : isSignUp ? "Create Account" : "Sign In"}
            </button>
          </form>

          <div className="text-center mb-6">
            <button 
              onClick={() => {
                setIsSignUp(!isSignUp)
                setError(null)
              }}
              className="text-sm text-zinc-400 hover:text-white transition-colors"
            >
              {isSignUp ? "Already have an account? Sign In" : "Don't have an account? Sign Up"}
            </button>
          </div>

          <div className="relative mb-6">
            <div className="absolute inset-0 flex items-center">
              <div className="w-full border-t border-zinc-800"></div>
            </div>
            <div className="relative flex justify-center text-xs uppercase">
              <span className="bg-[#12121a] px-2 text-zinc-600">Or continue with</span>
            </div>
          </div>

          <a
            id="sign-in-button"
            href={`${apiUrl}/auth/login`}
            className="w-full flex items-center justify-center gap-3 bg-white/5 hover:bg-white/10 text-white font-medium 
                       px-6 py-3.5 rounded-xl border border-white/10 transition-all duration-200 active:scale-[0.98]"
          >
            {/* Google icon */}
            <svg className="w-5 h-5" viewBox="0 0 24 24">
              <path
                d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92a5.06 5.06 0 0 1-2.2 3.32v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.1z"
                fill="#4285F4"
              />
              <path
                d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"
                fill="#34A853"
              />
              <path
                d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z"
                fill="#FBBC05"
              />
              <path
                d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z"
                fill="#EA4335"
              />
            </svg>
            Google
          </a>
        </div>

        {/* Footer */}
        <p className="mt-8 text-center text-xs text-zinc-600">
          By signing in you agree to our{" "}
          <span className="text-zinc-400 hover:text-white cursor-pointer transition-colors">Terms</span>
          {" & "}
          <span className="text-zinc-400 hover:text-white cursor-pointer transition-colors">Privacy Policy</span>
        </p>
      </div>
    </div>
  )
}
