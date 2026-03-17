#!/usr/bin/env python3
"""
Multi-User Codex API Server

This server demonstrates how to build a web service on top of Codex + stdio_bus.
Multiple users can interact with Codex simultaneously, each with their own session.
"""

import asyncio
import json
import uuid
from typing import Dict, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


# --- Configuration ---
STDIOBUS_HOST = "127.0.0.1"
STDIOBUS_PORT = 9999


# --- Models ---
class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None


class ChatResponse(BaseModel):
    session_id: str
    response: dict
    worker_info: Optional[str] = None


# --- stdio_bus Connection Pool ---
class StdioBusConnection:
    """Manages a persistent TCP connection to stdio_bus daemon."""
    
    def __init__(self):
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.lock = asyncio.Lock()
        self.pending_requests: Dict[int, asyncio.Future] = {}
        self.request_id = 0
        self._read_task: Optional[asyncio.Task] = None
    
    async def connect(self):
        """Establish connection to stdio_bus."""
        self.reader, self.writer = await asyncio.open_connection(
            STDIOBUS_HOST, STDIOBUS_PORT
        )
        self._read_task = asyncio.create_task(self._read_loop())
        print(f"Connected to stdio_bus at {STDIOBUS_HOST}:{STDIOBUS_PORT}")
    
    async def disconnect(self):
        """Close connection."""
        if self._read_task:
            self._read_task.cancel()
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
    
    async def _read_loop(self):
        """Background task to read responses from stdio_bus."""
        try:
            while True:
                line = await self.reader.readline()
                if not line:
                    break
                
                try:
                    response = json.loads(line.decode())
                    request_id = response.get("id")
                    
                    if request_id and request_id in self.pending_requests:
                        self.pending_requests[request_id].set_result(response)
                except json.JSONDecodeError:
                    print(f"Invalid JSON from stdio_bus: {line}")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"Read loop error: {e}")
    
    async def send_request(self, session_id: str, method: str, params: dict = None) -> dict:
        """Send a JSON-RPC request and wait for response."""
        async with self.lock:
            self.request_id += 1
            request_id = self.request_id
        
        request = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "sessionId": session_id,
            "params": params or {}
        }
        
        # Create future for response
        future = asyncio.get_event_loop().create_future()
        self.pending_requests[request_id] = future
        
        try:
            # Send request
            line = json.dumps(request) + "\n"
            self.writer.write(line.encode())
            await self.writer.drain()
            
            # Wait for response with timeout
            response = await asyncio.wait_for(future, timeout=30.0)
            return response
        finally:
            self.pending_requests.pop(request_id, None)


# --- Global connection ---
stdiobus = StdioBusConnection()


# --- User Session Manager ---
class SessionManager:
    """Tracks user sessions and their initialization state."""
    
    def __init__(self):
        self.sessions: Dict[str, dict] = {}
    
    def create_session(self, user_id: str) -> str:
        """Create a new session for a user."""
        session_id = f"thread:{user_id}-{uuid.uuid4().hex[:8]}"
        self.sessions[session_id] = {
            "user_id": user_id,
            "initialized": False,
            "created_at": asyncio.get_event_loop().time()
        }
        return session_id
    
    def get_session(self, session_id: str) -> Optional[dict]:
        return self.sessions.get(session_id)
    
    def mark_initialized(self, session_id: str):
        if session_id in self.sessions:
            self.sessions[session_id]["initialized"] = True


sessions = SessionManager()


# --- FastAPI App ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await stdiobus.connect()
    yield
    # Shutdown
    await stdiobus.disconnect()


app = FastAPI(
    title="Multi-User Codex Service",
    description="Web API for multiple users to interact with Codex via stdio_bus",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", response_class=HTMLResponse)
async def home():
    """Serve simple web UI."""
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Codex Multi-User Demo</title>
        <style>
            body { font-family: system-ui; max-width: 800px; margin: 50px auto; padding: 20px; }
            .user-panel { border: 1px solid #ccc; padding: 20px; margin: 10px 0; border-radius: 8px; }
            .user-panel h3 { margin-top: 0; }
            input, button { padding: 10px; margin: 5px; }
            input { width: 60%; }
            button { cursor: pointer; background: #007bff; color: white; border: none; border-radius: 4px; }
            .response { background: #f5f5f5; padding: 10px; margin-top: 10px; border-radius: 4px; white-space: pre-wrap; }
            .session-id { font-size: 12px; color: #666; }
        </style>
    </head>
    <body>
        <h1>🤖 Codex Multi-User Demo</h1>
        <p>This demonstrates multiple users interacting with Codex simultaneously via stdio_bus.</p>
        
        <div class="user-panel" id="user-a">
            <h3>👤 User A</h3>
            <div class="session-id">Session: <span id="session-a">Not started</span></div>
            <input type="text" id="input-a" placeholder="Type a message...">
            <button onclick="sendMessage('a')">Send</button>
            <div class="response" id="response-a">Responses will appear here...</div>
        </div>
        
        <div class="user-panel" id="user-b">
            <h3>👤 User B</h3>
            <div class="session-id">Session: <span id="session-b">Not started</span></div>
            <input type="text" id="input-b" placeholder="Type a message...">
            <button onclick="sendMessage('b')">Send</button>
            <div class="response" id="response-b">Responses will appear here...</div>
        </div>
        
        <div class="user-panel" id="user-c">
            <h3>👤 User C</h3>
            <div class="session-id">Session: <span id="session-c">Not started</span></div>
            <input type="text" id="input-c" placeholder="Type a message...">
            <button onclick="sendMessage('c')">Send</button>
            <div class="response" id="response-c">Responses will appear here...</div>
        </div>
        
        <script>
            const sessions = {};
            
            async function sendMessage(user) {
                const input = document.getElementById(`input-${user}`);
                const response = document.getElementById(`response-${user}`);
                const sessionSpan = document.getElementById(`session-${user}`);
                const message = input.value.trim();
                
                if (!message) return;
                
                response.textContent = 'Sending...';
                
                try {
                    const res = await fetch('/chat', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({
                            message: message,
                            session_id: sessions[user] || null
                        })
                    });
                    
                    const data = await res.json();
                    sessions[user] = data.session_id;
                    sessionSpan.textContent = data.session_id;
                    response.textContent = JSON.stringify(data.response, null, 2);
                } catch (e) {
                    response.textContent = 'Error: ' + e.message;
                }
                
                input.value = '';
            }
        </script>
    </body>
    </html>
    """


@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """
    Send a message to Codex.
    
    If session_id is not provided, a new session is created.
    All messages with the same session_id go to the same Codex worker (session affinity).
    """
    # Get or create session
    if request.session_id and sessions.get_session(request.session_id):
        session_id = request.session_id
        session = sessions.get_session(session_id)
    else:
        # Create new session
        user_id = f"user-{uuid.uuid4().hex[:6]}"
        session_id = sessions.create_session(user_id)
        session = sessions.get_session(session_id)
        
        # Initialize the session with Codex
        init_response = await stdiobus.send_request(
            session_id=session_id,
            method="initialize",
            params={"clientInfo": {"name": "multi-user-demo", "version": "1.0"}}
        )
        
        if "error" in init_response:
            raise HTTPException(status_code=500, detail=init_response["error"])
        
        sessions.mark_initialized(session_id)
    
    # For this demo, we just echo back via initialize
    # In a real app, you'd use thread/start, turn/start, etc.
    response = await stdiobus.send_request(
        session_id=session_id,
        method="initialize",  # Using initialize as a simple ping
        params={"clientInfo": {"name": "multi-user-demo", "version": "1.0"}}
    )
    
    return ChatResponse(
        session_id=session_id,
        response=response,
        worker_info=f"Session routed via stdio_bus"
    )


@app.get("/sessions")
async def list_sessions():
    """List all active sessions."""
    return {"sessions": sessions.sessions}


@app.get("/health")
async def health():
    """Health check."""
    return {"status": "ok", "stdiobus_connected": stdiobus.writer is not None}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
