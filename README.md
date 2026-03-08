<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/7379ec51-06a2-45ab-850c-8200fe75d1a6" />
## Inspiration
The job search process for many looks like the following:

- Job seekers send hundreds of applications with little response
- Many job postings online are old or not actively hiring
- Job postings are saturated, meaning realistically, companies aren't even reviewing most applications

The thing is, job seekers do not even realize they might already know someone at a company they want to work for. And if qualified, they could skip the resume screening process and get a direct interview with a company through their network.

LinkedIn is great for this, but with non-premium accounts, only a finite number of profile lookups can be made before one's quota is exhausted. That is why we created __Networkify__ to transform one's professional network data into an interactive graph that visualizes how people and companies are connected, for free! This allows users to quickly identify who they know at a company and the strongest referral path through first-, second-, or third-degree connections to reach the company they are interested in joining.

## Features
- 🔗**Referral Path Engine**
Finds the strongest path to employees or recruiters at a target company while supporting first, second, and third-degree connections.
- 🧠 **Network Graph Visualization**
An interactive 3D graph built with Three.js that visually maps relationships between people and companies, allowing users to view connection details, company info, and perform outreach.
- 🧑‍💼 **Recruiter-Only Filtering**
A graph with all branches pruned, except those consisting of connections to recruiters for speedy networking!
- 🌍 **Multi-Network Expansion**
Upload multiple connection datasets to expand and connect networks across teams, friends, and orgs. Essentially, reveals hidden opportunities through your primary connections.
- ☕ **AI Coffee Chat Assistant**
Generates personalized networking and introduction messages that can be immediately sent via email.

## How we built it
This project involved the use of AI. But that is the reality of software engineering today. We believe in understanding and working with AI rather than blindly copying from it.

**Frontend Stack**
- Next.js
- React
- Tailwind CSS
- Three.js
- React-force-graph
- Axios

**Backend Stack**
- Python
- FastAPI
- Pandas
- Authlib

**Database**
- Neo4j + built-in cache for rapid retrieval of nodes and relationships

**AI**
- Gemini API (Gemini 2.0 Flash)

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
