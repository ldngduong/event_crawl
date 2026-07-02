import asyncio
import sys

import uvicorn


def proactor_loop_factory(use_subprocess: bool = False):
    if sys.platform == "win32":
        return asyncio.ProactorEventLoop
    return asyncio.new_event_loop


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8006,
        loop="run_server:proactor_loop_factory",
        reload=False,
    )
