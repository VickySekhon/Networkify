"use client"
import { useState } from "react"
import { useRouter, usePathname } from "next/navigation"
import Link from "next/link"
import axios from "axios"
import { Upload, FileText, ArrowRight, Shield, CheckCircle2, Loader2, AlertCircle } from "lucide-react"
import Sidebar from "@/components/Sidebar"
import { useAuth } from "@/components/AuthContext"


export default function UploadPage() {
  const router = useRouter()
  const { user } = useAuth()
  const [file, setFile] = useState<File | null>(null)
  const [userName, setUserName] = useState(user?.name || "")
  const [status, setStatus] = useState<"idle" | "loading" | "done" | "error">("idle")
  const [result, setResult] = useState<any>(null)
  const [dragActive, setDragActive] = useState(false)

  const handleUpload = async () => {
    if (!file || !userName) return
    setStatus("loading")
    const formData = new FormData()
    formData.append("file", file)

    try {
      const res = await axios.post(
        `${process.env.NEXT_PUBLIC_API_URL}/api/upload/csv?user_name=${encodeURIComponent(userName)}`,
        formData,
        { headers: { "Content-Type": "multipart/form-data" } }
      )
      setResult(res.data)
      setStatus("done")
      localStorage.setItem("user_id", res.data.user_id)
      localStorage.setItem("user_name", userName)
    } catch (e) {
      console.error(e)
      setStatus("error")
    }
  }

  const handleDrag = (e: React.DragEvent) => {
    e.preventDefault()
    e.stopPropagation()
    if (e.type === "dragenter" || e.type === "dragover") {
      setDragActive(true)
    } else if (e.type === "dragleave") {
      setDragActive(false)
    }
  }

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault()
    e.stopPropagation()
    setDragActive(false)
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      setFile(e.dataTransfer.files[0])
    }
  }

  return (
    <div className="flex h-screen bg-dark-bg overflow-hidden">
      <Sidebar />

      <main className="flex-1 flex items-center justify-center p-6">
        <div className="w-full max-w-lg animate-fade-in">
        <div className="text-center mb-10">
          <div className="w-16 h-16 rounded-2xl bg-gradient-to-br from-brand-500 to-accent-cyan flex items-center justify-center mx-auto mb-6 shadow-glow">
            <Upload className="w-8 h-8 text-white" />
          </div>
          <h1 className="text-3xl font-bold text-white mb-3">Upload Your Network</h1>
          <p className="text-zinc-400">
            Export your connections from{" "}
            <span className="text-zinc-200">LinkedIn Settings → Data Privacy → Get a copy</span>
          </p>
        </div>

        {status === "done" ? (
          <div className="glass-card p-8 border-accent-emerald/30 animate-slide-up">
            <div className="text-center">
              <div className="w-16 h-16 rounded-full bg-accent-emerald/10 border border-accent-emerald/20 flex items-center justify-center mx-auto mb-6">
                <CheckCircle2 className="w-8 h-8 text-accent-emerald" />
              </div>
              <h2 className="text-2xl font-semibold text-white mb-2">Network Graph Built!</h2>
              <p className="text-zinc-400 mb-8">
                {result ? result.message : "Your connections have been mapped and analyzed by AI."}
              </p>
              
              <div className="grid gap-4">
                <button 
                  onClick={() => router.push("/search")} 
                  className="btn-primary"
                >
                  Search Company
                </button>
                <button 
                  onClick={() => router.push("/dashboard")} 
                  className="btn-secondary"
                >
                  View Dashboard
                </button>
                <button 
                  onClick={() => router.push("/graph")} 
                  className="btn-tertiary"
                >
                  Create Graph
                </button>
              </div>
            </div>
          </div>
        ) : (
          <div className="glass-card p-8 space-y-6">
            <div>
              <label className="block text-sm font-medium text-zinc-300 mb-2">
                Your Name
              </label>
              <input
                type="text"
                placeholder="As it appears on LinkedIn"
                value={userName}
                onChange={e => setUserName(e.target.value)}
                className="input-field"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-zinc-300 mb-2">
                Connections CSV
              </label>
              <label 
                className={`
                  relative flex flex-col items-center justify-center p-10 border-2 border-dashed rounded-xl 
                  cursor-pointer transition-all duration-200
                  ${dragActive 
                    ? "border-brand-500 bg-brand-500/10" 
                    : file 
                      ? "border-accent-emerald/50 bg-accent-emerald/5" 
                      : "border-dark-glassBorder hover:border-zinc-600 hover:bg-dark-elevated/50"
                  }
                `}
                onDragEnter={handleDrag}
                onDragLeave={handleDrag}
                onDragOver={handleDrag}
                onDrop={handleDrop}
              >
                <input 
                  type="file" 
                  accept=".csv" 
                  className="hidden" 
                  onChange={e => setFile(e.target.files?.[0] || null)} 
                />
                
                {file ? (
                  <div className="flex items-center gap-3">
                    <div className="w-10 h-10 rounded-lg bg-accent-emerald/10 flex items-center justify-center">
                      <FileText className="w-5 h-5 text-accent-emerald" />
                    </div>
                    <div className="text-left">
                      <p className="text-white font-medium">{file.name}</p>
                      <p className="text-sm text-zinc-500">Click or drag to replace</p>
                    </div>
                  </div>
                ) : (
                  <>
                    <Upload className={`w-8 h-8 mb-3 transition-colors ${dragActive ? "text-brand-400" : "text-zinc-500"}`} />
                    <p className="text-zinc-300 font-medium mb-1">
                      Drop your CSV here or click to browse
                    </p>
                    <p className="text-sm text-zinc-500">Connections.csv from LinkedIn export</p>
                  </>
                )}
              </label>
            </div>

            <button
              onClick={handleUpload}
              disabled={!file || !userName || status === "loading"}
              className="btn-primary w-full py-4 disabled:opacity-50 disabled:cursor-not-allowed disabled:shadow-none"
            >
              {status === "loading" ? (
                <>
                  <Loader2 className="w-5 h-5 animate-spin" />
                  Building Graph...
                </>
              ) : (
                <>
                  Build Network Graph
                  <ArrowRight className="w-5 h-5" />
                </>
              )}
            </button>

            <div className="flex items-center justify-center gap-2 text-xs text-zinc-500">
              <Shield className="w-4 h-4 text-accent-emerald" />
              <span>Your data is processed securely and never shared</span>
            </div>
          </div>
        )}

          <div className="mt-8 p-4 rounded-xl bg-dark-surface/50 border border-dark-glassBorder">
            <div className="flex items-start gap-3">
              <AlertCircle className="w-5 h-5 text-accent-amber mt-0.5 flex-shrink-0" />
              <div className="text-sm">
                <p className="text-zinc-300 font-medium mb-1">How to get your LinkedIn data</p>
                <p className="text-zinc-500">
                  Go to LinkedIn → Settings → Data Privacy → Get a copy of your data → 
                  Select &quot;Connections&quot; → Download when ready
                </p>
              </div>
            </div>
          </div>
        </div>
      </main>
    </div>
  )
}
