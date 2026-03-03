"""
Health Check and Monitoring Routes

Endpoints for system health, metrics, and monitoring.
"""

from datetime import datetime
from typing import Any, Dict

from fastapi import APIRouter, HTTPException

from app.services.live_data_manager import live_data_manager
from app.services.local_monitor import local_monitor
from app.services.strategy_manager import strategy_manager
from app.services.telegram_notifier import telegram_notifier


router = APIRouter(tags=["Monitoring"])


@router.get("/health")
async def health_check() -> Dict[str, Any]:
    """
    Comprehensive health check endpoint.

    Returns system status including:
    - Strategy status
    - Live data streaming status
    - Telegram notifications status
    - Broker connection status
    """
    try:
        # Get strategy state
        state = strategy_manager.get_state()

        # Get live data status
        live_data_status = {
            "is_streaming": live_data_manager.is_streaming,
            "last_update": live_data_manager.last_update.isoformat()
            if live_data_manager.last_update
            else None,
            "positions_count": len(live_data_manager.get_positions()),
            "orders_count": len(live_data_manager.get_orders()),
        }

        # Get Telegram status
        telegram_status = {
            "enabled": telegram_notifier.is_enabled,
        }

        # Determine overall health
        is_healthy = True
        issues = []

        if state.status.value == "ERROR":
            is_healthy = False
            issues.append(f"Strategy error: {state.error_message}")

        if live_data_manager.is_streaming and not live_data_manager.last_update:
            is_healthy = False
            issues.append("Live data stream not updating")

        return {
            "status": "healthy" if is_healthy else "unhealthy",
            "timestamp": datetime.utcnow().isoformat(),
            "strategy": {
                "status": state.status.value,
                "uptime": state.uptime,
                "error": state.error_message,
            },
            "live_data": live_data_status,
            "notifications": telegram_status,
            "issues": issues if issues else None,
        }

    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Health check failed: {str(e)}")


@router.get("/metrics")
async def get_metrics() -> Dict[str, Any]:
    """
    Get trading metrics and performance statistics.

    Returns session metrics including:
    - Total trades
    - Win/loss counts
    - PnL statistics
    - Active trades
    """
    try:
        session_summary = local_monitor.get_session_summary()
        active_trades = local_monitor.get_active_trades()

        return {
            "timestamp": datetime.utcnow().isoformat(),
            "session": session_summary,
            "active_trades": active_trades,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get metrics: {str(e)}")


@router.get("/dashboard")
async def get_dashboard() -> Dict[str, Any]:
    """
    Get dashboard data for frontend.

    Combines health, metrics, and live data into single response.
    """
    try:
        # Get all data
        state = strategy_manager.get_state()
        session_summary = local_monitor.get_session_summary()

        # Get live positions and orders
        positions = (
            live_data_manager.get_positions() if live_data_manager.is_streaming else []
        )
        orders = (
            live_data_manager.get_orders() if live_data_manager.is_streaming else []
        )

        return {
            "timestamp": datetime.utcnow().isoformat(),
            "strategy": {
                "status": state.status.value,
                "uptime": state.uptime,
                "error": state.error_message,
            },
            "performance": session_summary,
            "positions": positions,
            "orders": orders,
            "notifications": {
                "telegram_enabled": telegram_notifier.is_enabled,
            },
        }

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get dashboard data: {str(e)}"
        )


@router.post("/monitor/start")
async def start_monitoring() -> Dict[str, str]:
    """Start monitoring session."""
    try:
        local_monitor.start_session()
        return {
            "status": "success",
            "message": "Monitoring session started",
            "timestamp": datetime.utcnow().isoformat(),
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to start monitoring: {str(e)}"
        )


@router.post("/monitor/end")
async def end_monitoring() -> Dict[str, Any]:
    """End monitoring session and return summary."""
    try:
        session_metrics = local_monitor.end_session()

        return {
            "status": "success",
            "message": "Monitoring session ended",
            "summary": {
                "duration": str(session_metrics.end_time - session_metrics.start_time)
                if session_metrics.end_time
                else "N/A",
                "total_trades": session_metrics.total_trades,
                "winning_trades": session_metrics.winning_trades,
                "losing_trades": session_metrics.losing_trades,
                "win_rate": f"{session_metrics.win_rate:.1f}%",
                "total_pnl": f"₹{session_metrics.total_pnl:.2f}",
                "profit_factor": f"{session_metrics.profit_factor:.2f}",
            },
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to end monitoring: {str(e)}"
        )


@router.get("/monitor/status")
async def get_monitor_status() -> Dict[str, Any]:
    """Get current monitoring status."""
    try:
        summary = local_monitor.get_session_summary()
        active_trades = local_monitor.get_active_trades()

        return {
            "is_active": local_monitor._session is not None,
            "summary": summary,
            "active_trades": active_trades,
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get monitor status: {str(e)}"
        )
