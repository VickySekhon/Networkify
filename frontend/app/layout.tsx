import type { Metadata } from "next"
import "./globals.css"

export const metadata: Metadata = {
  title: "Pathfinder | Find Your Warmest Path to Any Company",
  description: "Upload your LinkedIn connections and discover hidden referral paths, rank contacts, and craft outreach messages with AI.",
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="min-h-screen bg-dark-bg">
        {children}
      </body>
    </html>
  )
}
