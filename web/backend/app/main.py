"""
Survivor Trading Strategy - FastAPI Backend

Main application entry point with API routes and WebSocket support.
"""
import asyncio
import json
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Dict, Any, List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.routes import strategy, config, positions, market, analysis, greeks, auth, strategy_selector
from app.websocket.manager import (
    connection_manager,
    create_price_update,
    create_strategy_state_update,
    create_heartbeat
)
from app.services.strategy_manager import strategy_manager
from app.services.broker_service import broker_service


# =============================================================================
# Lifespan Context Manager
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager.
    
    Handles startup and shutdown events.
    """
    # Startup
    print(f"Starting {settings.APP_NAME} v{settings.APP_VERSION}")
    print(f"Strategy config path: {settings.STRATEGY_CONFIG_PATH}")
    
    # Start background tasks
    heartbeat_task = asyncio.create_task(heartbeat_loop())
    state_broadcast_task = asyncio.create_task(state_broadcast_loop())
    
    yield
    
    # Shutdown
    print("Shutting down...")
    heartbeat_task.cancel()
    state_broadcast_task.cancel()
    
    # Stop strategy if running
    if strategy_manager.get_state().status.value == "RUNNING":
        strategy_manager.stop()


# =============================================================================
# FastAPI Application
# =============================================================================

app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="""
    API for the Survivor Trading Strategy.
    
    ## Features
    
    * **Strategy Control**: Start, stop, and monitor the trading strategy
    * **Configuration**: View and update strategy parameters
    * **Positions & Orders**: Track current positions and order history
    * **Market Data**: Real-time quotes and NIFTY index data
    * **WebSocket**: Real-time updates for prices, orders, and strategy state
    """,
    lifespan=lifespan
)


# =============================================================================
# CORS Middleware
# =============================================================================

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# Include Routers
# =============================================================================

app.include_router(strategy.router, prefix="/api")
app.include_router(strategy_selector.router, prefix="/api")  # New strategy selector API
app.include_router(config.router, prefix="/api")
app.include_router(positions.router, prefix="/api")
app.include_router(market.router, prefix="/api")
app.include_router(analysis.router)
app.include_router(greeks.router)
app.include_router(auth.router, prefix="/api")


# =============================================================================
# WebSocket Endpoint
# =============================================================================

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for real-time data streaming.
    
    Message Types (Server -> Client):
    - PRICE_UPDATE: Real-time price updates
    - STRATEGY_STATE: Strategy state changes
    - ORDER_UPDATE: Order status updates
    - POSITION_UPDATE: Position changes
    - HEARTBEAT: Connection keepalive
    
    Message Types (Client -> Server):
    - SUBSCRIBE: Subscribe to symbols
    - UNSUBSCRIBE: Unsubscribe from symbols
    """
    await connection_manager.connect(websocket)
    
    try:
        # Send initial state
        state = strategy_manager.get_state()
        await connection_manager.send_personal_message(
            create_strategy_state_update(state.model_dump()),
            websocket
        )
        
        while True:
            # Receive and process messages from client
            data = await websocket.receive_text()
            
            try:
                message = json.loads(data)
                await handle_websocket_message(websocket, message)
            except json.JSONDecodeError:
                await connection_manager.send_personal_message(
                    {"type": "ERROR", "data": {"message": "Invalid JSON"}},
                    websocket
                )
                
    except WebSocketDisconnect:
        connection_manager.disconnect(websocket)
    except Exception as e:
        print(f"WebSocket error: {e}")
        connection_manager.disconnect(websocket)


async def handle_websocket_message(websocket: WebSocket, message: Dict[str, Any]) -> None:
    """
    Handle incoming WebSocket messages.
    
    Args:
        websocket: The WebSocket connection
        message: The received message
    """
    msg_type = message.get("type", "").upper()
    
    if msg_type == "SUBSCRIBE":
        symbols = message.get("symbols", [])
        if symbols:
            connection_manager.subscribe(websocket, symbols)
            await connection_manager.send_personal_message(
                {"type": "SUBSCRIBED", "data": {"symbols": symbols}},
                websocket
            )
            
    elif msg_type == "UNSUBSCRIBE":
        symbols = message.get("symbols", [])
        if symbols:
            connection_manager.unsubscribe(websocket, symbols)
            await connection_manager.send_personal_message(
                {"type": "UNSUBSCRIBED", "data": {"symbols": symbols}},
                websocket
            )
            
    elif msg_type == "PING":
        await connection_manager.send_personal_message(
            {"type": "PONG", "data": {"time": datetime.utcnow().isoformat()}},
            websocket
        )
        
    else:
        await connection_manager.send_personal_message(
            {"type": "ERROR", "data": {"message": f"Unknown message type: {msg_type}"}},
            websocket
        )


# =============================================================================
# Background Tasks
# =============================================================================

async def heartbeat_loop():
    """
    Background task to send periodic heartbeats.
    """
    while True:
        try:
            await asyncio.sleep(settings.WS_HEARTBEAT_INTERVAL)
            await connection_manager.broadcast(create_heartbeat())
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"Heartbeat error: {e}")


async def state_broadcast_loop():
    """
    Background task to broadcast strategy state updates.
    """
    while True:
        try:
            await asyncio.sleep(1)  # Update every second
            
            # Only broadcast if strategy is running
            state = strategy_manager.get_state()
            if state.status.value == "RUNNING":
                await connection_manager.broadcast(
                    create_strategy_state_update(state.model_dump())
                )
                
                # Also broadcast NIFTY price update
                try:
                    nifty_data = broker_service.get_nifty_data(
                        pe_reference=state.nifty_pe_last_value,
                        ce_reference=state.nifty_ce_last_value
                    )
                    await connection_manager.broadcast(
                        create_price_update(
                            symbol="NSE:NIFTY 50",
                            last_price=nifty_data.quote.last_price,
                            change=nifty_data.quote.change
                        )
                    )
                except Exception:
                    pass  # Ignore quote errors
                    
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"State broadcast error: {e}")


# =============================================================================
# Health Check
# =============================================================================

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "version": settings.APP_VERSION,
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "name": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "docs": "/docs",
        "websocket": "/ws"
    }


# =============================================================================
# Main Entry Point
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG
    )
