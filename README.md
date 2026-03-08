# Networkify

<img width="944" height="753" alt="Screenshot 2026-03-08 041726" src="https://github.com/user-attachments/assets/d4cbffee-d349-49f4-985f-ec394d99cd0f" />

**Networkify** is a networking-powered job discovery platform that helps people find opportunities through their professional network instead of blindly applying to jobs. Unlike LinkedIn, which simply lists connections, Networkify transforms professional network data into an interactive graph that visualizes how people and companies are connected. This allows users to instantly see who they know at a company and identify the **strongest referral path** through first, second or third-degree connections to reach the right person.

---

## Inspiration
The job search process for many looks like the following:

- Job seekers send hundreds of applications with little response
- Many job postings online are old or not actively hiring
- Job postings are saturated, meaning realistically, companies aren't even reviewing most applications

The thing is, job seekers do not even realize they might already know someone at a company they want to work for. And if qualified, they could skip the resume screening process and get a direct interview with a company through their network.

LinkedIn is great for this, but with non-premium accounts, only a finite number of profile lookups can be made before one's quota is exhausted. That is why we created __Networkify__ to transform one's professional network data into an interactive graph that visualizes how people and companies are connected, for free! This allows users to quickly identify who they know at a company and the strongest referral path through first-, second-, or third-degree connections to reach the company they are interested in joining.

## Features
🔗**Referral Path Engine**
Finds the strongest path to employees or recruiters at a target company while supporting first, second, and third-degree connections.
🧠 **Network Graph Visualization**
An interactive 3D graph built with Three.js that visually maps relationships between people and companies, allowing users to view connection details, company info, and perform outreach.
🧑‍💼 **Recruiter-Only Filtering**
A graph with all branches pruned, except those consisting of connections to recruiters for speedy networking!
🌍 **Multi-Network Expansion**
Upload multiple connection datasets to expand and connect networks across teams, friends, and orgs. Essentially, reveals hidden opportunities through your primary connections.
☕ **AI Coffee Chat Assistant**
Generates personalized networking and introduction messages that can be immediately sent via email.

## How we built it
This project involved the use of AI. But that is the reality of software engineering today. We believe in understanding and working with AI rather than blindly copying from it.

**Frontend Stack**
Next.js
React
Tailwind CSS
Three.js
React-force-graph
Axios

**Backend Stack**
Python
FastAPI
Pandas
Authlib

**Database**
Neo4j + built-in cache for rapid retrieval of nodes and relationships

**AI**
Gemini API (Gemini 2.0 Flash)

## Challenges we ran into
One of our biggest challenges was getting all the moving pieces to work together across a full-stack application under time pressure. We ran into environment variable mismatches between our .env files and our Pydantic config (e.g., NEO4J_USERNAME vs NEO4J_USER, and a logo.dev secret key sk_ being baked into 500+ Neo4j records instead of the public key pk_), which caused silent failures that were difficult to trace. Deploying the frontend to Vercel while the backend lived on a teammate's Google Cloud Run project introduced Auth0 callback URL mismatches and CORS considerations. On the frontend, building a performant force-directed graph visualization with clustering, drag-pinning, and 2D/3D toggling pushed us into performance bottlenecks, a full-screen backdrop-blur overlay was freezing the UI during mode switches, and the backend's synchronous Clearbit API calls were blocking FastAPI's async event loop for hundreds of seconds during CSV uploads with many unique companies.

## Accomplishments that we're proud of
We built a fully functional, end-to-end LinkedIn network intelligence tool in a single hackathon. Users can upload their LinkedIn connections CSV, which is widely available for all LinkedIn users, and within seconds see an interactive, visually rich graph that is complete with company logos, recruiter badges, initials on person nodes, and automatic clustering of large companies rendered in both 2D and 3D.

 We implemented smart graph merging that deduplicates people across multiple uploaded networks using email and name+company matching, a search and filter system that lets users drill into specific companies or recruiters, and an AI-powered message drafting feature. On the infrastructure side, we're proud of the performance optimizations we shipped: parallel company URL resolution with ThreadPoolExecutor, deduplicated API calls, non-blocking CSV processing via run_in_executor, and a lightweight mode-switching banner that doesn't tank frame rates.

## What we learned
We learned firsthand how many subtle issues arise when integrating multiple services such as Neo4j Aura, Auth0, logo.dev, Clearbit, Google Cloud Run, and Vercel that have their own authentication schemes. Small details like an env var name being off by one word, or a secret key vs public key prefix, can cause issues.

We also gained a deep appreciation for async architecture: a single synchronous request.get() inside a FastAPI async handler can starve the entire event loop, and the fix (thread pool executor + deduplication + parallelism) turned a worst-case 500-second upload into roughly 50 seconds. 

On the frontend, we learned that CSS effects like backdrop-blur have real GPU costs and that requestAnimationFrame is essential for giving the browser a chance to paint before heavy computation.

## What's next for Networkify
We want to expand Networkify into a true networking co-pilot by integrating real-time LinkedIn profile enrichment via Scrapfly to surface richer context like recent posts, mutual connections count, and activity signals. This would be done by using the LinkedIn API to access data without violating their terms of service and facing legal consequences from scraping their data, which they guardrail behind captchas and other modern web scraping prevention tools. 

We plan to add a recommendation engine that proactively suggests whom to reach out to based on relevance scoring, and a Chrome extension that overlays Networkify insights directly on LinkedIn profile pages. On the infrastructure side, we want to move to WebSocket-based graph updates so multiple team members can collaboratively map their networks in real time, and implement proper CI/CD with GitHub Actions, deploying to Vercel and Cloud Run automatically on every push.
## Sponsor Integrations

- **Google Antigravity**  
  The project was developed using Google's Antigravity IDE, which accelerated development by providing an integrated AI-powered coding environment for rapid prototyping and iteration.

- **Auth0**  
  Handles authentication and identity management, allowing users to securely log in and maintain protected profiles while ensuring sensitive data remains safe.

- **Gemini API**  
  Powers the AI Coffee Chat Assistant, generating personalized networking and introduction messages with a single click.

- **Tailscale**  
  Used for secure deployment and networking between services, ensuring safe and reliable communication across the application's infrastructure.
---

## Quick Start

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
