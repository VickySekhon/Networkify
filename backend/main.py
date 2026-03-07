from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.routes import upload, search, graph, enrich, messages

app = FastAPI(title="LinkedIn PathFinder API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(upload.router,   prefix="/api/upload",   tags=["Upload"])
app.include_router(search.router,   prefix="/api/search",   tags=["Search"])
app.include_router(graph.router,    prefix="/api/graph",    tags=["Graph"])
app.include_router(enrich.router,   prefix="/api/enrich",   tags=["Enrich"])
app.include_router(messages.router, prefix="/api/messages", tags=["Messages"])

@app.get("/health")
def health():
    return {"status": "ok"}
