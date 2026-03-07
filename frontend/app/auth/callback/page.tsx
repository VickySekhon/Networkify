"use client"
import { Suspense, useEffect, useRef, useState } from "react"
import { useRouter, useSearchParams } from "next/navigation"
import { useAuth } from "@/components/AuthContext"
import { Loader2, CheckCircle2 } from "lucide-react"

function CallbackHandler() {
  const router = useRouter()
  const searchParams = useSearchParams()
  const { login } = useAuth()
  const [status, setStatus] = useState<"processing" | "success" | "error">("processing")
  const [errorMsg, setErrorMsg] = useState("")
  const hasRun = useRef(false)

  useEffect(() => {
    // Guard: only run once
    if (hasRun.current) return
    hasRun.current = true

    const token = searchParams.get("token")
    const userB64 = searchParams.get("user")

    if (!token || !userB64) {
      setStatus("error")
      setErrorMsg("Missing authentication data. Please try signing in again.")
      return
    }

    try {
      // Decode URL-safe base64 user info
      const userJson = atob(userB64.replace(/-/g, "+").replace(/_/g, "/"))
      const user = JSON.parse(userJson)

      // Store auth data
      login(token, user)
      setStatus("success")

      // Redirect after a brief moment
      setTimeout(() => {
        router.push("/upload")
      }, 800)
    } catch (err) {
      console.error("Auth callback error:", err)
      setStatus("error")
      setErrorMsg("Failed to process authentication. Please try again.")
    }
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <div className="min-h-screen bg-[#0a0a12] flex items-center justify-center">
      <div className="text-center">
        {status === "processing" && (
          <>
            <Loader2 className="w-10 h-10 text-brand-400 animate-spin mx-auto mb-4" />
            <p className="text-zinc-400 text-sm">Signing you in...</p>
          </>
        )}
        {status === "success" && (
          <>
            <CheckCircle2 className="w-10 h-10 text-accent-emerald mx-auto mb-4" />
            <p className="text-white font-medium mb-1">Welcome!</p>
            <p className="text-zinc-500 text-sm">Redirecting to your dashboard...</p>
          </>
        )}
        {status === "error" && (
          <div className="max-w-sm mx-auto">
            <div className="w-10 h-10 rounded-full bg-red-500/10 border border-red-500/20 flex items-center justify-center mx-auto mb-4">
              <span className="text-red-400 text-xl">!</span>
            </div>
            <p className="text-white font-medium mb-2">Authentication Failed</p>
            <p className="text-zinc-500 text-sm mb-6">{errorMsg}</p>
            <a
              href="/login"
              className="inline-flex items-center gap-2 px-5 py-2.5 rounded-xl bg-brand-600 hover:bg-brand-500 text-white text-sm font-medium transition-all"
            >
              Back to Login
            </a>
          </div>
        )}
      </div>
    </div>
  )
}

export default function AuthCallbackPage() {
  return (
    <Suspense
      fallback={
        <div className="min-h-screen bg-[#0a0a12] flex items-center justify-center">
          <Loader2 className="w-10 h-10 text-brand-400 animate-spin" />
        </div>
      }
    >
      <CallbackHandler />
    </Suspense>
  )
}
