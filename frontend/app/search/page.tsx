"use client"
import { usePathname } from "next/navigation"
import { Waypoints, Sparkles, MessageSquare, Copy, ChevronLeft, User, Building2, ArrowRight, CheckCircle2, Home, Users, Upload, Search, Settings, Loader2, RefreshCw, Mail } from "lucide-react"
import Link from "next/link"
import Sidebar from "@/components/Sidebar"
import { useState } from "react"
import axios from "axios"

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000"


interface Contact {
  id?: string
  name: string
  title?: string
  company?: string
  email?: string
  degree: number
  relevance_score: number
  is_recruiter?: boolean
  bridge?: { name: string; title?: string }
}

interface SearchResults {
  company: string
  total_connections: number
  first_degree_count: number
  second_degree_count: number
  recruiters: Contact[]
  top_connections: Contact[]
}

export default function SearchPage() {
  const [query, setQuery] = useState("")
  const [loading, setLoading] = useState(false)
  const [results, setResults] = useState<SearchResults | null>(null)
  const [selectedContact, setSelectedContact] = useState<Contact | null>(null)
  const [aiMessage, setAiMessage] = useState("")
  const [messageLoading, setMessageLoading] = useState(false)
  const [copiedMessage, setCopiedMessage] = useState(false)
  const [emailError, setEmailError] = useState(false)

  const search = async () => {
    if (!query) return
    setLoading(true)
    setResults(null)
    setSelectedContact(null)
    setAiMessage("")
    setEmailError(false)
    const userId = localStorage.getItem("user_id") || ""
    const userName = localStorage.getItem("user_name") || ""
    try {
      const res = await axios.get(`${API_URL}/api/search/company`, {
        params: { company: query, user_id: userId, user_name: userName }
      })
      setResults(res.data)
      if (res.data.top_connections?.length > 0) {
        setSelectedContact(res.data.top_connections[0])
      }
    } catch (e) {
      console.error("Search failed:", e)
    } finally {
      setLoading(false)
    }
  }

  const generateMessage = async (contact: Contact) => {
    setSelectedContact(contact)
    setMessageLoading(true)
    setAiMessage("")
    setEmailError(false)
    const userName = localStorage.getItem("user_name") || "Me"
    try {
      const res = await axios.post(`${API_URL}/api/messages/generate`, {
        user: { name: userName, companies: [], schools: [] },
        target_person: contact,
        target_company: query,
        bridge_person: contact.bridge || null,
      })
      setAiMessage(res.data.message)
    } catch (e) {
      setAiMessage(`Error: ${(e as any)?.response?.data?.detail || "Could not generate message. Try again in a moment."}`)
    } finally {
      setMessageLoading(false)
    }
  }

  const handleCopy = () => {
    navigator.clipboard.writeText(aiMessage)
    setCopiedMessage(true)
    setTimeout(() => setCopiedMessage(false), 2000)
  }

  const bestContact = results?.top_connections?.[0] || null

  return (
    <div className="flex h-screen bg-dark-bg overflow-hidden">
      <Sidebar />

      <main className="flex-1 p-6 overflow-auto">
        <div className="max-w-5xl mx-auto animate-fade-in">
          <div className="sticky top-0 z-30 bg-dark-bg/95 backdrop-blur-sm -mx-6 px-6 pt-2 pb-6 border-b border-dark-glassBorder mb-8">
            <Link href="/dashboard" className="inline-flex items-center gap-1.5 text-sm text-zinc-500 hover:text-white transition-colors mb-4">
              <ChevronLeft className="w-4 h-4" />
              Back to Dashboard
            </Link>
            <div className="flex flex-col md:flex-row md:items-end justify-between gap-4">
              <div>
                <h1 className="text-3xl font-bold text-white mb-2">Find Your Path</h1>
                <p className="text-zinc-400">Search a company to discover your warmest connections.</p>
              </div>

              <div className="flex gap-3 w-full md:w-auto md:min-w-[400px]">
                <input
                  value={query}
                  onChange={e => setQuery(e.target.value)}
                  onKeyDown={e => e.key === "Enter" && search()}
                  placeholder="e.g. Google, Shopify, OpenAI..."
                  className="flex-1 bg-dark-surface border border-dark-glassBorder rounded-xl px-4 py-3 text-white placeholder-zinc-500 focus:outline-none focus:border-brand-500 transition-colors"
                />
                <button onClick={search} disabled={loading || !query} className="btn-primary px-6 py-3 disabled:opacity-40">
                  {loading ? (<><Loader2 className="w-5 h-5 animate-spin" />Searching...</>) : (<><Search className="w-5 h-5" />Search</>)}
                </button>
              </div>
            </div>
          </div>

          {results && (
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
              <div className="lg:col-span-2 space-y-6">
                <div className="grid grid-cols-3 gap-4">
                  {[
                    { label: "Total Connections", value: results.total_connections },
                    { label: "Direct (1st°)", value: results.first_degree_count },
                    { label: "Via Referral (2nd°)", value: results.second_degree_count },
                  ].map(s => (
                    <div key={s.label} className="bg-dark-surface rounded-2xl p-4 text-center">
                      <div className="text-2xl font-bold text-brand-400">{s.value}</div>
                      <div className="text-xs text-zinc-500 mt-1">{s.label}</div>
                    </div>
                  ))}
                </div>

                {bestContact && (
                  <div className="glass-card p-6">
                    <div className="flex items-center gap-2 mb-6">
                      <Waypoints className="w-5 h-5 text-brand-400" />
                      <h2 className="text-lg font-semibold text-white">Best Referral Path</h2>
                    </div>
                    <div className="bg-dark-bg/50 rounded-xl p-8 border border-dark-glassBorder relative overflow-hidden">
                      <div className="flex items-center justify-center gap-4 sm:gap-8">
                        <div className="text-center">
                          <div className="w-14 h-14 rounded-full bg-gradient-to-br from-brand-500 to-accent-cyan flex items-center justify-center mx-auto mb-3 shadow-glow">
                            <User className="w-6 h-6 text-white" />
                          </div>
                          <p className="text-sm font-medium text-white">You</p>
                        </div>
                        {bestContact.degree === 2 && bestContact.bridge && (
                          <>
                            <div className="flex-1 max-w-[80px] flex flex-col items-center">
                              <div className="h-0.5 w-full bg-gradient-to-r from-brand-500 to-purple-500 rounded-full" />
                              <span className="text-[10px] text-zinc-500 mt-2">1st degree</span>
                            </div>
                            <div className="text-center">
                              <div className="w-14 h-14 rounded-full bg-purple-500/20 border-2 border-purple-500 flex items-center justify-center mx-auto mb-3">
                                <User className="w-6 h-6 text-purple-400" />
                              </div>
                              <p className="text-sm font-medium text-white">{bestContact.bridge.name.split(" ")[0]}</p>
                              <p className="text-xs text-purple-400">Bridge</p>
                            </div>
                          </>
                        )}
                        <div className="flex-1 max-w-[80px] flex flex-col items-center">
                          <div className="h-0.5 w-full bg-gradient-to-r from-purple-500 to-accent-cyan rounded-full" />
                          <span className="text-[10px] text-zinc-500 mt-2">{bestContact.degree === 1 ? "1st degree" : "referral"}</span>
                        </div>
                        <div className="text-center">
                          <div className="w-14 h-14 rounded-full bg-accent-emerald/20 border-2 border-accent-emerald flex items-center justify-center mx-auto mb-3">
                            <User className="w-6 h-6 text-accent-emerald" />
                          </div>
                          <p className="text-sm font-medium text-white">{bestContact.name.split(" ")[0]}</p>
                          <p className="text-xs text-accent-emerald">{(bestContact.title || "").split(" ").slice(0,2).join(" ")}</p>
                        </div>
                        <div className="flex-1 max-w-[80px] flex flex-col items-center">
                          <div className="h-0.5 w-full bg-gradient-to-r from-accent-emerald to-accent-cyan rounded-full" />
                          {/* <span className="text-[10px] text-zinc-500 mt-2">works at</span> */}
                        </div>
                        <div className="text-center">
                          <div className="w-14 h-14 rounded-full bg-accent-cyan/20 border-2 border-accent-cyan flex items-center justify-center mx-auto mb-3">
                            <Building2 className="w-6 h-6 text-accent-cyan" />
                          </div>
                          <p className="text-sm font-medium text-white">{results.company}</p>
                          <p className="text-xs text-accent-cyan">Target</p>
                        </div>
                      </div>
                      <div className="mt-6 pt-6 border-t border-dark-glassBorder">
                        <p className="text-sm text-zinc-400 text-center">
                          <span className="text-accent-emerald font-medium">{Math.round(bestContact.relevance_score * 100)}% match</span> — {bestContact.is_recruiter ? "Recruiter at the company" : `${bestContact.degree === 1 ? "Direct" : "2nd-degree"} connection at ${results.company}`}
                        </p>
                      </div>
                    </div>
                  </div>
                )}

                {results.recruiters?.length > 0 && (
                  <div className="glass-card p-6">
                    <div className="flex items-center gap-2 mb-6">
                      <Sparkles className="w-5 h-5 text-accent-amber" />
                      <h2 className="text-lg font-semibold text-white">🎯 Recruiters at {results.company}</h2>
                    </div>
                    <div className="space-y-4">
                      {results.recruiters.map((contact, idx) => (
                        <ContactRow key={`r-${idx}`} contact={contact} onDraftMessage={() => generateMessage(contact)} />
                      ))}
                    </div>
                  </div>
                )}

                <div className="glass-card p-6">
                  <div className="flex items-center gap-2 mb-6">
                    <Sparkles className="w-5 h-5 text-accent-amber" />
                    <h2 className="text-lg font-semibold text-white">Recommended Contacts</h2>
                  </div>
                  <div className="space-y-4">
                    {results.top_connections.map((contact, idx) => (
                      <ContactRow key={`c-${idx}`} contact={contact} onDraftMessage={() => generateMessage(contact)} />
                    ))}
                    {results.top_connections.length === 0 && (
                      <p className="text-zinc-500 text-center py-8">No connections found at this company.</p>
                    )}
                  </div>
                </div>
              </div>

              <div className="space-y-6">
                <div className="glass-card p-6">
                  <div className="flex items-center gap-2 mb-4">
                    <MessageSquare className="w-5 h-5 text-accent-cyan" />
                    <h3 className="text-lg font-semibold text-white">AI Message</h3>
                  </div>
                  {selectedContact ? (
                    <>
                      <p className="text-sm text-zinc-400 mb-3">Message for <span className="text-white">{selectedContact.name}</span></p>
                      {messageLoading ? (
                        <div className="bg-dark-bg/80 rounded-xl p-4 border border-dark-glassBorder h-40 flex items-center justify-center">
                          <Loader2 className="w-6 h-6 animate-spin text-brand-400 mx-auto" />
                        </div>
                      ) : aiMessage ? (
                        <>
                          <textarea
                            value={aiMessage}
                            onChange={e => setAiMessage(e.target.value)}
                            rows={7}
                            className="w-full bg-dark-bg/80 rounded-xl p-4 mb-4 border border-dark-glassBorder text-sm text-zinc-300 leading-relaxed focus:outline-none focus:border-brand-500 resize-none transition-colors"
                          />
                          <div className="flex gap-2">
                            <button
                              onClick={handleCopy}
                              className={`flex-1 text-sm py-2.5 rounded-xl font-medium transition-all inline-flex items-center justify-center gap-2 ${copiedMessage ? "bg-accent-emerald text-white" : "bg-brand-600 hover:bg-brand-500 text-white"}`}
                            >
                              {copiedMessage ? (<><CheckCircle2 className="w-4 h-4" />Copied!</>) : (<><Copy className="w-4 h-4" />Copy</>)}
                            </button>
                            <button
                              onClick={() => {
                                if (selectedContact?.email) {
                                  window.location.href = `mailto:${selectedContact.email}?subject=${encodeURIComponent("Quick Coffee Chat?")}&body=${encodeURIComponent(aiMessage)}`
                                  setEmailError(false)
                                } else {
                                  setEmailError(true)
                                }
                              }}
                              className="border border-dark-glassBorder hover:border-zinc-500 text-zinc-400 hover:text-white text-sm px-4 py-2.5 rounded-xl font-medium transition-all inline-flex items-center justify-center gap-2"
                            >
                              <Mail className="w-4 h-4" />Send Email
                            </button>
                          </div>
                          {emailError && (
                            <p className="mt-2 text-xs text-accent-rose text-center">
                              Email could not be found, copy this message to send directly.
                            </p>
                          )}
                        </>
                      ) : (
                        <button onClick={() => generateMessage(selectedContact)} className="w-full bg-brand-600 hover:bg-brand-500 text-white text-sm py-3 rounded-xl font-medium transition-all inline-flex items-center justify-center gap-2">
                          Generate Outreach Message
                        </button>
                      )}
                    </>
                  ) : (
                    <p className="text-sm text-zinc-500 text-center py-8">Search a company and click &quot;Draft Message&quot; to generate an AI outreach message.</p>
                  )}
                  <p className="text-xs text-zinc-500 text-center mt-3">Powered by Gemini</p>
                </div>
              </div>
            </div>
          )}

          {!results && !loading && (
            <div className="text-center py-20">
              <div className="w-20 h-20 rounded-2xl bg-dark-surface border border-dark-glassBorder flex items-center justify-center mx-auto mb-6">
                <Search className="w-8 h-8 text-zinc-600" />
              </div>
              <h3 className="text-xl font-semibold text-zinc-400 mb-2">Search for a company</h3>
              <p className="text-zinc-500 text-sm">Enter a company name above to find your warmest paths in.</p>
            </div>
          )}
        </div>
      </main>
    </div>
  )
}

function ContactRow({ contact, onDraftMessage }: { contact: Contact; onDraftMessage: () => void }) {
  const matchPercent = Math.round(contact.relevance_score * 100)
  return (
    <div className="p-4 rounded-xl bg-dark-bg/50 border border-dark-glassBorder hover:border-brand-500/30 transition-all group">
      <div className="flex flex-col sm:flex-row sm:items-start justify-between gap-4">
        <div className="flex gap-4">
          <div className="w-12 h-12 rounded-full bg-dark-elevated flex items-center justify-center flex-shrink-0">
            <User className="w-5 h-5 text-zinc-400" />
          </div>
          <div>
            <div className="flex items-center gap-2 mb-1 flex-wrap">
              <h4 className="font-semibold text-white">{contact.name}</h4>
              <span className={`text-xs px-2 py-0.5 rounded-full ${matchPercent >= 70 ? "bg-accent-emerald/10 text-accent-emerald border border-accent-emerald/20" : matchPercent >= 40 ? "bg-brand-500/10 text-brand-400 border border-brand-500/20" : "bg-zinc-500/10 text-zinc-400 border border-zinc-500/20"}`}>
                {matchPercent}% match
              </span>
              <span className="text-xs px-2 py-0.5 rounded-full bg-dark-elevated text-zinc-500 border border-dark-glassBorder">{contact.degree === 1 ? "1st" : "2nd"}°</span>
              {contact.is_recruiter && <span className="text-xs px-2 py-0.5 rounded-full bg-accent-amber/10 text-accent-amber border border-accent-amber/20">Recruiter</span>}
            </div>
            <p className="text-sm text-zinc-400 mb-1">{contact.title || "Unknown role"}</p>
            {contact.degree === 2 && contact.bridge && <p className="text-xs text-zinc-600">Via {contact.bridge.name}</p>}
          </div>
        </div>
        <button onClick={onDraftMessage} className="btn-primary text-sm px-4 py-2 flex-shrink-0">
          Draft Message<ArrowRight className="w-4 h-4" />
        </button>
      </div>
    </div>
  )
}
