import os

import uvicorn


if __name__ == "__main__":
    os.environ.setdefault("OPENAI_API_KEY", "dummy")
    os.environ.setdefault("OPENROUTER_API_KEY", "dummy")

    uvicorn.run(
        "main:app",
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8006")),
        reload=os.getenv("RELOAD", "true").lower() in {"1", "true", "yes"},
    )
