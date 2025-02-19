import logging
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, AnyHttpUrl, validator
from typing import Optional
from datetime import datetime
from perplex import MultiUrlProcessor, StreamLogger
import json
from sse_starlette.sse import EventSourceResponse
import asyncio
import hashlib
import uvicorn
import os

# Configure logging
logging.basicConfig(
    level=logging.WARNING,  # Changed to WARNING to reduce log volume
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler()  # Remove file handler to avoid disk usage
    ]
)
logger = logging.getLogger(__name__)

class URLRequest(BaseModel):
    url: AnyHttpUrl
    query: Optional[str] = ""

    @validator('url')
    def validate_url(cls, v):
        if not str(v).startswith(('http://', 'https://')):
            raise ValueError('URL must start with http:// or https://')
        return str(v)

app = FastAPI(
    title="Web Summarizer API",
    description="Optimized API for URL processing and summarization",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global processor instance
processor = None

@app.on_event("startup")
async def startup_event():
    global processor
    stream_logger = StreamLogger()
    processor = MultiUrlProcessor(
        google_api_key="AIzaSyDFcr4m2w64aiFpHb6pctxutXNAh8MICeQ",
        search_engine_id="e121a1ca931084c94",
        stream_logger=stream_logger,
    )

@app.on_event("shutdown")
async def shutdown_event():
    if processor and processor.session:
        await processor.close_session()

@app.post("/api/url/stream")
async def stream_url_process(request: URLRequest):
    if not processor:
        logger.error("Processor not initialized")
        raise HTTPException(status_code=500, detail="Processor not initialized")

    cache_key = hashlib.md5(f"{request.url}:{request.query}".encode()).hexdigest()
    
    # Check cache
    cached_result = await processor.cache.get(cache_key)
    if cached_result:
        async def cached_generator():
            # Send info about cached result
            yield {
                "event": "processing",
                "data": json.dumps({
                    "type": "info",
                    "message": "Retrieved from cache",
                    "cached": True
                })
            }
            # Send the content chunk
            yield {
                "event": "summary",
                "data": json.dumps({
                    "type": "chunk",
                    "content": cached_result['content'],
                    "cached": True
                })
            }
            # Signal completion
            yield {
                "event": "complete",
                "data": json.dumps({
                    "type": "success",
                    "content": cached_result['content'],
                    "timestamp": datetime.now().isoformat(),
                    "cached": True
                })
            }
        return EventSourceResponse(cached_generator())

    log_queue = asyncio.Queue()
    chunks = []

    async def event_generator():
        try:
            async for chunk in processor.process_single_url_stream(
                str(request.url), 
                request.query, 
                log_queue
            ):
                chunks.append(chunk)
                yield {
                    "event": "summary",
                    "data": json.dumps({
                        "type": "chunk",
                        "content": chunk
                    })
                }

            final_content = "".join(chunks)
            await processor.cache.set(cache_key, {
                "content": final_content,
                "timestamp": datetime.now().isoformat()
            })

            yield {
                "event": "complete",
                "data": json.dumps({
                    "type": "success",
                    "timestamp": datetime.now().isoformat()
                })
            }

        except Exception as e:
            logger.exception("Error in stream_url_process")
            yield {
                "event": "error",
                "data": json.dumps({
                    "type": "error",
                    "message": str(e)
                })
            }

    return EventSourceResponse(event_generator())

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(
        "optimized_backend:app", 
        host="0.0.0.0", 
        port=port,
        workers=1,  # Single worker for free tier
        loop="auto",
        limit_concurrency=5  # Limit concurrent connections
    )
