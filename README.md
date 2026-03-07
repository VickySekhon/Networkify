# 🚀 Network Smarter (HackCanada2026)

*A Web App that helps users network smarter by turning their LinkedIn Connections CSV into a searchable, actionable company graph.*

You upload your contacts, search a target company (e.g. Google), and the app maps the most efficient connections, recruiters to contact, and even drafts the perfect AI-generated outreach message.

---

## 🏗 Why It Wins (Top 3 Pitch Points)

1. **Referral Path Engine**: Finds the smartest path through your network to reach a company, mapped beautifully through 1st- and 2nd-degree connections.
2. **Networking Strategy Engine**: Ranks the best people to contact based on relationship strength and generates exact reasoning for why they are the best fit.
3. **Multi-Source Career Graph**: Builds a unified professional network using Neo4j graph databases rather than static SQL tables.

## 💻 Tech Stack & API Sponsors

We built this project specifically keeping the sponsor tracks and minimal, highly-effective tech in mind.

### Core Stack
- **Frontend**: Next.js 14, React, Tailwind CSS (Glassmorphism UI)
- **Backend**: Python, FastAPI, Pandas (for heavy CSV processing)
- **Database**: Neo4j (Graph Database for People → Company relationships)

### Sponsor Integrations
- **Cloudinary ("Relationship Map")**: Dynamically generates unique, branded OG images of a user's specific "Google Connection Path" so they can share their referral routes.
- **Auth0 ("Shadow Auth")**: Identity management that explicitly matches login emails against the uploaded CSV data for access control.
- **Vultr ("The Cold Start Worker")**: Cloud instance dedicated to running the heavy Pandas DataFrame processing to keep the frontend completely fluid.
- **ElevenLabs ("The Pitch Coach")**: Generates an audio snippet of how the user should verbally introduce themselves if they meet their networking contact in person.
- **Gemini API ("Ghost Job Analyst")**: Uses Gemini's large context window to compare job posting URLs against the user's network graph to detect "Ghost Jobs" (roles that no one in the network actually knows about).
- **Tailscale ("Secure Lab")**: Secures the transfer of PII (Personally Identifiable Information) from the local machine to the Vultr instance via a Tailscale Funnel.

---

## 📁 Repository Structure

```
HackCanada2026/
├── frontend/             # Next.js UI (Dashboard, Referral Paths, Upload)
├── backend/              # FastAPI & Pandas Processing (Python)
├── neo4j/                # Graph Database configuration
├── extension/            # Chrome Extension for 2nd-degree LinkedIn traversal
├── docker-compose.yml    # Local neo4j container definition
└── README.md             # You are here
```

## 🚀 Quick Start

```bash
# 1. Start Neo4j Database
docker-compose up -d neo4j

# 2. Start Python Backend
cd backend
pip install -r requirements.txt
uvicorn main:app --reload

# 3. Start Next.js Frontend
cd frontend
npm install
npm run dev
```

*Built with ❤️ at HackCanada2026*