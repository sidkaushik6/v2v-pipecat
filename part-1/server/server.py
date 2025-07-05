# server.py

import uvicorn
import os
import logging
import subprocess
import asyncio
import websockets
from fastapi import FastAPI, WebSocket, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from dotenv import load_dotenv

from pipecat.transports.network.fastapi_websocket import (
    FastAPIWebsocketTransport,
    FastAPIWebsocketParams
)

from pipecat.serializers.twilio import TwilioFrameSerializer

load_dotenv(override=True)
logger = logging.getLogger("pc")

bot_procs = {}        # pid -> (process, conn_id)
connections = {}      # conn_id -> user WebSocket
bot_ws_map = {}       # conn_id -> bot WebSocket

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def serve_ui():
    return FileResponse("frontend/index.html")

@app.websocket("/ws")
async def user_ws(websocket: WebSocket):
    await websocket.accept()
    conn_id = id(websocket)
    connections[conn_id] = websocket

    try:
        proc = subprocess.Popen(
            ["python3", "-m", "bot", "--connection_id", str(conn_id)],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            shell=False
        )
        bot_procs[proc.pid] = (proc, conn_id)
        logger.info(f"Started bot process {proc.pid} for connection {conn_id}")
    except Exception as e:
        logger.error(f"Bot startup failed: {e}")
        await websocket.close(code=1011, reason="Bot startup failed")
        return

    try:
        while True:
            data = await websocket.receive_bytes()
            # Forward binary frames to bot
            if conn_id in bot_ws_map:
                await bot_ws_map[conn_id].send_bytes(data)
            # Optionally log VAD signals
            if data.startswith(b"VAD:"):
                status = data.decode().split(":", 1)[1]
                await websocket.send_text(f"VAD_{status}")
    except:
        logger.info(f"User connection {conn_id} closed")
    finally:
        connections.pop(conn_id, None)
        proc, _ = bot_procs.pop(proc.pid, (None, None))
        if proc:
            proc.terminate()
        bot_ws_map.pop(conn_id, None)

# @app.websocket("/bot_ws/{conn_id}")
# async def bot_ws(websocket: WebSocket, conn_id: str):
#     await websocket.accept()
#     conn_id = int(conn_id)
#     if conn_id not in connections:
#         await websocket.close(code=1008, reason="Invalid connection ID")
#         return

#     bot_ws_map[conn_id] = websocket
#     user_ws = connections[conn_id]

#     # Wrap transport around bot->server WebSocket
#     transport = FastAPIWebsocketTransport(
#         websocket=websocket,
#         params=FastAPIWebsocketParams(
#             audio_in_enabled=True,
#             audio_out_enabled=True,
#             vad_enabled=True,
#             serializer=TwilioFrameSerializer(stream_sid="dummy")
#         )
#     )

    # # Forward audio in both directions
    # async def recv_from_bot():
    #     async for frame in transport.input():
    #         await user_ws.send_bytes(frame)

    # async def send_to_bot():
    #     async for frame in transport.output():
    #         await websocket.send_bytes(frame)

    # try:
    #     await asyncio.gather(recv_from_bot(), send_to_bot())
    # finally:
    #     logger.info(f"Bot connection closed for {conn_id}")
    #     bot_ws_map.pop(conn_id, None)
    
    
    
    
    
    # In server.py - bot_ws endpoint
@app.websocket("/bot_ws/{conn_id_str}")
async def bot_ws(websocket: WebSocket, conn_id_str: str):
    try:
        conn_id = int(conn_id_str)
        if conn_id not in connections:
            await websocket.close(code=1008, reason="Invalid connection ID")
            return
            
        await websocket.accept()
        bot_ws_map[conn_id] = websocket
        user_ws = connections[conn_id]

        transport = FastAPIWebsocketTransport(
            websocket=websocket,
            params=FastAPIWebsocketParams(
                audio_in_enabled=True,
                audio_out_enabled=True,
                serializer=TwilioFrameSerializer(stream_sid="dummy")
            )
        )

        # Define tasks with proper error handling
        async def recv_from_bot():
            try:
                while True:
                    try:
                        frame = await transport.receive_frame()
                        if frame:
                            await user_ws.send_bytes(frame)
                    except websockets.exceptions.ConnectionClosedOK:
                        logger.info("Bot connection closed normally (recv)")
                        break
                    except websockets.exceptions.ConnectionClosedError as e:
                        logger.error(f"Bot connection closed with error (recv): {e}")
                        break
                    except Exception as e:
                        logger.error(f"Error receiving from bot: {e}")
                        # Continue for transient errors
            except asyncio.CancelledError:
                logger.info("recv_from_bot task cancelled")
            except Exception as e:
                logger.error(f"Critical error in recv_from_bot: {e}")

        async def send_to_bot():
            try:
                while True:
                    try:
                        # Use queue.get() with timeout
                        frame = await asyncio.wait_for(transport.output().get(), timeout=1.0)
                        if frame:
                            await websocket.send_bytes(frame)
                    except asyncio.TimeoutError:
                        # Normal timeout, continue
                        continue
                    except websockets.exceptions.ConnectionClosedOK:
                        logger.info("Bot connection closed normally (send)")
                        break
                    except websockets.exceptions.ConnectionClosedError as e:
                        logger.error(f"Bot connection closed with error (send): {e}")
                        break
                    except Exception as e:
                        logger.error(f"Error sending to bot: {e}")
            except asyncio.CancelledError:
                logger.info("send_to_bot task cancelled")
            except Exception as e:
                logger.error(f"Critical error in send_to_bot: {e}")

        # Create and run tasks
        recv_task = asyncio.create_task(recv_from_bot())
        send_task = asyncio.create_task(send_to_bot())
        
        try:
            await asyncio.gather(recv_task, send_task)
        except asyncio.CancelledError:
            logger.info("WebSocket tasks cancelled")
        finally:
            # Cancel tasks if still running
            if not recv_task.done():
                recv_task.cancel()
            if not send_task.done():
                send_task.cancel()
    except Exception as e:
        logger.error(f"Unexpected error in bot_ws: {e}")
    finally:
        # Cleanup outside the task context
        if 'conn_id' in locals():
            logger.info(f"Bot connection closed for {conn_id}")
            bot_ws_map.pop(conn_id, None)
    
    

@app.get("/status/{pid}")
def status(pid: int):
    proc_info = bot_procs.get(pid)
    if not proc_info:
        raise HTTPException(status_code=404, detail="Bot not found")
    proc = proc_info[0]
    state = "running" if proc.poll() is None else "finished"
    return JSONResponse({"bot_id": pid, "status": state})

if __name__ == "__main__":
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("FAST_API_PORT", "7860"))
    uvicorn.run("server:app", host=host, port=port, reload=False)
