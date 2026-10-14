"""Real Uvicorn multiprocess supervisor; mock-only test resources and stop-file shutdown."""

import argparse
import asyncio
import os
import threading
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from starlette.websockets import WebSocket
from uvicorn.supervisors import Multiprocess

from marketdata.api import create_app
from marketdata.config import Settings
from marketdata.persistence import Store
from marketdata.providers.base import MarketDataProvider


def factory():
    def forbidden(*args, **kwargs):
        raise AssertionError("API worker attempted provider construction or registration")

    MarketDataProvider.__init__ = forbidden
    Store.register = forbidden
    app = create_app(Settings(_env_file=None))
    original_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def lifespan(application):
        async with original_lifespan(application):
            ready = Path(os.environ["TEST_WORKER_STOP_FILE"]).parent / f"worker-{os.getpid()}.ready"
            await asyncio.to_thread(ready.touch)
            yield

    app.router.lifespan_context = lifespan
    original_tick = uvicorn.Server.on_tick
    original_accept = WebSocket.accept
    stopped = threading.Event()
    accept_paused = False

    def watch_worker():
        stop_file = Path(os.environ["TEST_WORKER_STOP_FILE"])
        while not stopped.wait(0.05):
            if stop_file.exists():
                stopped.set()

    threading.Thread(target=watch_worker, daemon=True).start()

    async def tick(self, counter):
        nonlocal accept_paused
        directory = Path(os.environ["TEST_WORKER_STOP_FILE"]).parent
        if not accept_paused and (directory / f"worker-{os.getpid()}.pause-accept").exists():
            # Close only this worker's listening handle. Existing HTTP/WS connections
            # stay alive; the other worker retains its copy of the shared listener.
            for listener in self.servers:
                listener.close()
            accept_paused = True
            (directory / f"worker-{os.getpid()}.accept-paused").touch()
        return stopped.is_set() or await original_tick(self, counter)

    async def accept(self, subprotocol=None, headers=None):
        # Test-only evidence identifying which actual worker owns this connection.
        headers = list(headers or []) + [(b"x-test-worker", str(os.getpid()).encode())]
        await original_accept(self, subprotocol=subprotocol, headers=headers)

    uvicorn.Server.on_tick = tick
    WebSocket.accept = accept
    return app


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--stop-file", type=Path, required=True)
    args = parser.parse_args()
    os.environ["TEST_WORKER_STOP_FILE"] = str(args.stop_file)
    config = uvicorn.Config(
        "process_market_workers:factory",
        factory=True,
        host="127.0.0.1",
        port=args.port,
        workers=2,
        ws="websockets",
        ws_max_size=65536,
        ws_max_queue=16,
        log_level="error",
    )
    listener = config.bind_socket()

    class TestSupervisor(Multiprocess):
        def terminate_all(self):
            # Each worker's normal Server.on_tick sees the stop file. This avoids
            # sending Windows console signals to unrelated development terminals.
            args.stop_file.touch()

        def join_all(self):
            failures = []
            for process in self.processes:
                process.process.join(20)
                if process.process.is_alive():
                    process.kill()
                    process.join()
                    failures.append(process.pid)
                elif process.exitcode != 0:
                    failures.append(process.pid)
            if failures:
                raise RuntimeError(f"test workers failed graceful shutdown: {failures}")

    supervisor = TestSupervisor(config, [listener])

    def watch():
        while not supervisor.should_exit.wait(0.05):
            if args.stop_file.exists():
                supervisor.should_exit.set()

    watcher = threading.Thread(target=watch, daemon=True)
    watcher.start()
    try:
        supervisor.run()
    finally:
        supervisor.should_exit.set()
        watcher.join(2)
        listener.close()


if __name__ == "__main__":
    main()
