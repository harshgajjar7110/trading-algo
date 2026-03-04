"""
Strategy Manager Service

Manages the lifecycle of trading strategies.
Handles starting, stopping, and monitoring strategy processes.
Supports both single strategy mode (backward compatible) and multi-strategy mode.
"""

import asyncio
import logging
import multiprocessing
import os
import sys
import time
import yaml
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Callable

logger = logging.getLogger(__name__)

# Add project root to Python path for imports
_project_root = Path(__file__).resolve().parents[4]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from app.config import StrategyConfig, settings
from app.models.schemas import StrategyState, StrategyStatus
from app.services.strategy_registry import (
    StrategyRegistry,
    StrategyInstance,
    StrategyType,
)
from app.services.live_data_manager import live_data_manager
from app.services.telegram_notifier import telegram_notifier


class StrategyManager:
    """
    Unified Strategy Manager supporting both single and multi-strategy modes.

    Features:
    - Backward compatible single strategy mode (Survivor)
    - Multi-strategy support with registry
    - Strategy selection, configuration, and confirmation flow
    - Per-strategy config management
    - Clear identification of running strategy
    """

    _instance: Optional["StrategyManager"] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._initialized = True
        self._registry = StrategyRegistry()

        # Current state
        self._process: Optional[multiprocessing.Process] = None
        self._state_queue: Optional[multiprocessing.Queue] = None
        self._current_instance: Optional[StrategyInstance] = None

        # Status tracking
        self._status = StrategyStatus.STOPPED
        self._start_time: Optional[float] = None
        self._error_message: Optional[str] = None

        # State cache for quick access
        self._nifty_pe_last_value: Optional[float] = None
        self._nifty_ce_last_value: Optional[float] = None
        self._pe_reset_flag: bool = False
        self._ce_reset_flag: bool = False
        self._last_update: Optional[datetime] = None

        # Visual state cache for Visual Engine
        self._visual_state_cache: Optional[Dict[str, Any]] = None
        self._last_visual_state_update: Optional[datetime] = None

        # Callbacks for state changes
        self._on_state_change: Optional[Callable] = None

        # Legacy config support (for backward compatibility)
        self._config: Optional[StrategyConfig] = None
        self._load_legacy_config()

    # ==================== Legacy/Backward Compatible Methods ====================

    def _load_legacy_config(self) -> Optional[StrategyConfig]:
        """Load configuration from YAML file (backward compatibility)."""
        config_path = settings.STRATEGY_CONFIG_PATH

        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                config_data = yaml.safe_load(f)
                if config_data and "default" in config_data:
                    self._config = StrategyConfig(**config_data["default"])
        else:
            self._config = StrategyConfig()

        return self._config

    def get_config(self) -> StrategyConfig:
        """Get current configuration (backward compatibility)."""
        if self._config is None:
            self._load_legacy_config()
        return self._config

    def update_config(self, updates: Dict[str, Any]) -> StrategyConfig:
        """
        Update configuration with new values (backward compatibility).

        Args:
            updates: Dictionary of configuration updates

        Returns:
            Updated configuration
        """
        if self._config is None:
            self._load_legacy_config()

        for key, value in updates.items():
            if value is not None and hasattr(self._config, key):
                setattr(self._config, key, value)

        self._save_legacy_config()
        return self._config

    def _save_legacy_config(self) -> None:
        """Save current configuration to YAML file (backward compatibility)."""
        config_path = settings.STRATEGY_CONFIG_PATH
        config_data = {"default": self._config.model_dump()}
        os.makedirs(os.path.dirname(config_path), exist_ok=True)

        with open(config_path, "w") as f:
            yaml.dump(config_data, f, default_flow_style=False)

    # ==================== Multi-Strategy Methods ====================

    def get_available_strategies(self) -> Dict:
        """Get list of available strategies for selection."""
        return {
            "strategies": self._registry.get_strategy_comparison(),
            "current": self.get_current_strategy_info(),
        }

    def get_strategy_details(self, strategy_id: str) -> Optional[Dict]:
        """Get detailed info about a specific strategy."""
        info = self._registry.get(strategy_id)
        if not info:
            return None

        config = self._registry.load_config(strategy_id)

        return {
            "id": info.id,
            "name": info.name,
            "description": info.description,
            "version": info.version,
            "risk_level": info.risk_level,
            "recommended_capital": info.recommended_capital,
            "tags": info.tags,
            "default_params": info.default_params,
            "current_config": config,
            "config_path": info.config_path,
        }

    def preview_strategy_config(
        self, strategy_id: str, config_override: Optional[Dict] = None
    ) -> Dict:
        """
        Preview the configuration that will be used when starting a strategy.
        This allows users to confirm parameters before starting.
        """
        info = self._registry.get(strategy_id)
        if not info:
            return {"error": f"Strategy not found: {strategy_id}"}

        base_config = self._registry.load_config(strategy_id)
        preview_config = base_config.copy()
        if config_override:
            preview_config.update(config_override)

        errors = self._registry.validate_config(strategy_id, preview_config)

        differences = []
        for key, value in preview_config.items():
            if key in info.default_params and info.default_params[key] != value:
                differences.append(
                    {
                        "param": key,
                        "default": info.default_params[key],
                        "current": value,
                    }
                )

        sl_config = {
            "enabled": preview_config.get("sl_enabled", False),
            "percentage": preview_config.get("sl_percentage", 60),
            "order_type": preview_config.get("sl_order_type", "STOP_LIMIT"),
            "limit_buffer": preview_config.get("sl_limit_buffer", 5.0),
            "reconcile_on_start": preview_config.get("sl_reconcile_on_start", True),
        }

        sl_preview = self._calculate_sl_preview(preview_config)

        return {
            "strategy_id": strategy_id,
            "strategy_name": info.name,
            "config": preview_config,
            "is_valid": len(errors) == 0,
            "validation_errors": errors,
            "differences_from_default": differences,
            "risk_level": info.risk_level,
            "recommended_capital": info.recommended_capital,
            "sl_config": sl_config,
            "sl_preview": sl_preview,
            "requires_confirmation": True,
            "confirmation_message": "Review & Start Strategy",
        }

    def _calculate_sl_preview(self, config: Dict) -> Dict:
        """Calculate SL preview information based on configuration."""
        sl_percentage = config.get("sl_percentage", 60)
        sl_limit_buffer = config.get("sl_limit_buffer", 5.0)
        sl_enabled = config.get("sl_enabled", False)

        if not sl_enabled:
            return {
                "enabled": False,
                "message": "SL orders are disabled",
            }

        examples = []
        for entry_price in [50.0, 100.0, 150.0, 200.0]:
            trigger_price = round(entry_price * (1 + sl_percentage / 100), 2)
            limit_price = round(trigger_price + sl_limit_buffer, 2)
            examples.append(
                {
                    "entry_price": entry_price,
                    "trigger_price": trigger_price,
                    "limit_price": limit_price,
                    "potential_loss_pct": sl_percentage,
                }
            )

        return {
            "enabled": True,
            "sl_percentage": sl_percentage,
            "sl_limit_buffer": sl_limit_buffer,
            "example_calculations": examples,
            "message": f"SL will be placed at {sl_percentage}% above entry price for short positions",
        }

    # ==================== Start/Stop Methods ====================

    def start(
        self,
        strategy_id: Optional[str] = None,
        config_override: Optional[Dict] = None,
        confirmed: bool = False,
    ) -> Dict:
        """
        Start a strategy.

        Args:
            strategy_id: The strategy to start (required for multi-strategy, optional for legacy)
            config_override: Optional config overrides
            confirmed: Whether user has confirmed the config

        Returns:
            Dict with status and message
        """
        # Check if already running
        if self._status == StrategyStatus.RUNNING:
            current_id = self._registry.get_current_strategy_id() or "survivor"
            return {
                "success": False,
                "message": f"Strategy '{current_id}' is already running. Stop it first.",
                "requires_confirmation": False,
            }

        if self._status == StrategyStatus.STARTING:
            return {
                "success": False,
                "message": "Strategy is already starting...",
                "requires_confirmation": False,
            }

        # Legacy mode: if no strategy_id provided, default to survivor
        if strategy_id is None:
            strategy_id = "survivor"

        try:
            info = self._registry.get(strategy_id)
            if not info:
                return {
                    "success": False,
                    "message": f"Unknown strategy: {strategy_id}",
                    "requires_confirmation": False,
                }

            instance = self._registry.create_instance(strategy_id, config_override)

            if not confirmed:
                preview = self.preview_strategy_config(strategy_id, config_override)
                return {
                    "success": False,
                    "message": "Confirmation required",
                    "requires_confirmation": True,
                    "preview": preview,
                }

            self._status = StrategyStatus.STARTING
            self._error_message = None

            self._current_instance = instance
            self._registry.set_current_instance(instance)

            self._state_queue = multiprocessing.Queue()

            self._process = multiprocessing.Process(
                target=self._run_strategy_process,
                args=(strategy_id, instance.config, self._state_queue),
            )
            self._process.start()

            self._start_time = time.time()
            self._status = StrategyStatus.RUNNING
            instance.status = "RUNNING"
            instance.started_at = datetime.utcnow()
            instance.pid = self._process.pid

            # Start live data streaming
            async def _start_live_data():
                try:
                    await live_data_manager.start_stream()
                except Exception as e:
                    logger.error(f"Failed to start live data stream: {e}")

            asyncio.create_task(_start_live_data())

            # Send Telegram notification
            async def _notify_start():
                try:
                    await telegram_notifier.send_strategy_started(
                        strategy_name=info.name, config=instance.config
                    )
                except Exception as e:
                    logger.error(f"Failed to send start notification: {e}")

            asyncio.create_task(_notify_start())

            asyncio.create_task(self._monitor_state())

            return {
                "success": True,
                "message": f"Strategy '{info.name}' started successfully",
                "strategy_id": strategy_id,
                "instance_id": instance.id,
                "pid": instance.pid,
                "requires_confirmation": False,
            }

        except ValueError as e:
            self._status = StrategyStatus.ERROR
            self._error_message = str(e)
            return {
                "success": False,
                "message": f"Validation error: {str(e)}",
                "requires_confirmation": False,
            }
        except Exception as e:
            self._status = StrategyStatus.ERROR
            self._error_message = str(e)
            traceback.print_exc()
            return {
                "success": False,
                "message": f"Failed to start: {str(e)}",
                "requires_confirmation": False,
            }

    def stop(self) -> Dict:
        """Stop the currently running strategy."""
        if self._status == StrategyStatus.STOPPED:
            return {"success": True, "message": "Strategy already stopped"}

        if self._status == StrategyStatus.STOPPING:
            return {"success": False, "message": "Strategy is already stopping..."}

        try:
            self._status = StrategyStatus.STOPPING

            if self._process and self._process.is_alive():
                self._process.terminate()
                self._process.join(timeout=10)

                if self._process.is_alive():
                    self._process.kill()
                    self._process.join()

            if self._current_instance:
                self._current_instance.status = "STOPPED"
                self._current_instance.stopped_at = datetime.utcnow()

            # Stop live data streaming
            async def _stop_live_data():
                try:
                    await live_data_manager.stop_stream()
                except Exception as e:
                    logger.error(f"Failed to stop live data stream: {e}")

            asyncio.create_task(_stop_live_data())

            # Send Telegram notification
            async def _notify_stop():
                try:
                    if self._current_instance:
                        await telegram_notifier.send_strategy_stopped(
                            strategy_name=self._current_instance.strategy_type.value,
                            reason="User initiated stop"
                            if self._status != StrategyStatus.ERROR
                            else "Error occurred",
                        )
                except Exception as e:
                    logger.error(f"Failed to send stop notification: {e}")

            asyncio.create_task(_notify_stop())

            self._cleanup()

            return {"success": True, "message": "Strategy stopped successfully"}

        except Exception as e:
            self._status = StrategyStatus.ERROR
            self._error_message = str(e)
            return {"success": False, "message": f"Error stopping: {str(e)}"}

    def restart(
        self,
        strategy_id: Optional[str] = None,
        config_override: Optional[Dict] = None,
        confirmed: bool = False,
    ) -> Dict:
        """Restart strategy (optionally with different strategy/config)."""
        stop_result = self.stop()
        if not stop_result["success"]:
            return stop_result

        time.sleep(1)

        if strategy_id is None:
            if self._current_instance:
                strategy_id = self._current_instance.strategy_type.value
            else:
                strategy_id = "survivor"

        return self.start(strategy_id, config_override, confirmed)

    # ==================== State Methods ====================

    def get_state(self) -> StrategyState:
        """Get current strategy state."""
        uptime = None
        if self._start_time and self._status == StrategyStatus.RUNNING:
            uptime = time.time() - self._start_time

        current_strategy = "None"
        if self._current_instance:
            current_strategy = f"{self._current_instance.name} ({self._current_instance.strategy_type.value})"

        return StrategyState(
            status=self._status,
            nifty_pe_last_value=self._nifty_pe_last_value,
            nifty_ce_last_value=self._nifty_ce_last_value,
            pe_reset_flag=self._pe_reset_flag,
            ce_reset_flag=self._ce_reset_flag,
            last_update=self._last_update,
            error_message=self._error_message,
            uptime_seconds=uptime,
            current_strategy=current_strategy,
            instance_id=self._current_instance.id if self._current_instance else None,
        )

    def get_visual_state(self) -> Optional[Dict[str, Any]]:
        """
        Get visual state for the Visual Engine dashboard.
        
        Returns cached visual state if available, or generates a basic
        state from current strategy information.
        
        Returns:
            Dict containing visual state data, or None if strategy not running
        """
        if self._status != StrategyStatus.RUNNING:
            return None
        
        # Return cached visual state if available and fresh (< 30 seconds old)
        if self._visual_state_cache and self._last_visual_state_update:
            age_seconds = (datetime.utcnow() - self._last_visual_state_update).total_seconds()
            if age_seconds < 30:
                return self._visual_state_cache
        
        # Generate basic visual state from available data
        uptime = None
        if self._start_time:
            uptime = time.time() - self._start_time
        
        current_strategy = "Unknown"
        if self._current_instance:
            current_strategy = f"{self._current_instance.name} ({self._current_instance.strategy_type.value})"
        
        # Build basic visual state
        visual_state = {
            "is_running": True,
            "uptime_seconds": uptime,
            "current_strategy": current_strategy,
            "last_update": datetime.utcnow().isoformat(),
            "filters": {
                "entry_filter_type": "ALL",
                "items": []
            },
            "predictions": {
                "pe": None,
                "ce": None
            },
            "signals": [],
            "market_context": {
                "nifty_price": None,
                "trend": "NEUTRAL",
                "volatility_regime": "NORMAL",
                "gap_assessment": {
                    "can_trade": True,
                    "message": "Visual state loading..."
                }
            },
            "daily_stats": {
                "trades_taken": 0,
                "trades_rejected": 0,
                "pnl": 0.0,
                "consecutive_losses": 0
            },
            "message": "Visual state is initializing. Full data will be available shortly."
        }
        
        return visual_state

    def update_visual_state_cache(self, visual_state: Dict[str, Any]) -> None:
        """
        Update the visual state cache.
        
        This is called by the API endpoint or state processor when
        fresh visual state data is available from the strategy.
        
        Args:
            visual_state: The new visual state data
        """
        self._visual_state_cache = visual_state
        self._last_visual_state_update = datetime.utcnow()
        logger.debug(f"Visual state cache updated at {self._last_visual_state_update}")

    def get_current_strategy_info(self) -> Optional[Dict]:
        """Get info about currently running strategy."""
        if not self._current_instance:
            return None

        return {
            "id": self._current_instance.id,
            "strategy_id": self._current_instance.strategy_type.value,
            "name": self._current_instance.name,
            "status": self._current_instance.status,
            "started_at": self._current_instance.started_at.isoformat()
            if self._current_instance.started_at
            else None,
            "pid": self._current_instance.pid,
            "config_summary": {
                k: v
                for k, v in self._current_instance.config.items()
                if k
                in [
                    "symbol_initials",
                    "pe_gap",
                    "ce_gap",
                    "pe_quantity",
                    "ce_quantity",
                    "entry_filter_type",
                ]
            },
        }

    def _cleanup(self):
        """Clean up resources."""
        self._process = None
        self._state_queue = None
        self._status = StrategyStatus.STOPPED
        self._start_time = None
        self._registry.clear_current_instance()

    # ==================== Monitor Methods ====================

    async def _monitor_state(self):
        """Monitor state updates from strategy process."""
        while self._status == StrategyStatus.RUNNING:
            try:
                if self._state_queue and not self._state_queue.empty():
                    update = self._state_queue.get_nowait()
                    self._process_state_update(update)

                await asyncio.sleep(0.1)
            except Exception as e:
                print(f"[StrategyManager] State monitor error: {e}")
                await asyncio.sleep(1)

    def _process_state_update(self, update: Dict):
        """Process state update from strategy."""
        if update.get("type") == "STATE_UPDATE":
            self._nifty_pe_last_value = update.get("nifty_pe_last_value")
            self._nifty_ce_last_value = update.get("nifty_ce_last_value")
            self._pe_reset_flag = update.get("pe_reset_flag", False)
            self._ce_reset_flag = update.get("ce_reset_flag", False)
            self._last_update = datetime.utcnow()

        elif update.get("type") == "GAP_RISK_UPDATE":
            # Handle gap risk notifications from strategy
            gap_percent = update.get("gap_percent", 0.0)
            gift_nifty_gap = update.get("gift_nifty_gap")
            position_multiplier = update.get("position_multiplier", 1.0)
            trading_blocked = update.get("trading_blocked", False)
            reason = update.get("reason", "")
            
            # Send Telegram notification for significant gap risk events
            async def _notify_gap_risk():
                try:
                    await telegram_notifier.send_gap_risk_alert(
                        gap_percent=gap_percent,
                        gift_nifty_gap=gift_nifty_gap,
                        position_multiplier=position_multiplier,
                        trading_blocked=trading_blocked,
                        reason=reason,
                    )
                except Exception as e:
                    logger.error(f"Failed to send gap risk notification: {e}")
            
            asyncio.create_task(_notify_gap_risk())

        elif update.get("type") == "ERROR":
            error_msg = update.get("message", "Unknown error")
            self._error_message = error_msg
            self._status = StrategyStatus.ERROR

            if self._current_instance:
                self._current_instance.status = "ERROR"
                self._current_instance.error_message = error_msg

            # Send Telegram notification for errors
            async def _notify_error():
                try:
                    await telegram_notifier.send_error(
                        error_message=error_msg,
                        context=f"Strategy: {self._current_instance.strategy_type.value if self._current_instance else 'Unknown'}",
                    )
                except Exception as e:
                    logger.error(f"Failed to send error notification: {e}")

            asyncio.create_task(_notify_error())

            # Auto-stop on critical errors
            insufficient_fund_keywords = [
                "insufficient",
                "margin",
                "funds",
                "balance",
                "exposure",
                "limit exceeded",
                "not enough",
                "margin shortfall",
            ]
            if any(kw in error_msg.lower() for kw in insufficient_fund_keywords):
                self._stop_on_error(f"INSUFFICIENT FUNDS: {error_msg}")

    def _stop_on_error(self, error_message: str):
        """Stop strategy due to error."""
        try:
            if self._process and self._process.is_alive():
                self._process.terminate()
                self._process.join(timeout=5)
                if self._process.is_alive():
                    self._process.kill()
                    self._process.join()

            self._status = StrategyStatus.ERROR
            self._error_message = error_message

        except Exception as e:
            print(f"[StrategyManager] Error in emergency stop: {e}")

    # ==================== Strategy Process ====================

    @staticmethod
    def _run_strategy_process(
        strategy_id: str, config: Dict, state_queue: multiprocessing.Queue
    ):
        """Run strategy in separate process."""
        try:
            from brokers import BrokerGateway
            from dispatcher import DataDispatcher
            from orders import OrderTracker
            from queue import Queue
            from logger import setup_strategy_logging_with_name

            logger = setup_strategy_logging_with_name(strategy_id)
            logger.info("=" * 70)
            logger.info(f"STARTING STRATEGY: {strategy_id}")
            logger.info("=" * 70)

            registry = StrategyRegistry()
            strategy_class = registry.load_strategy_class(strategy_id)

            broker_name = os.getenv("BROKER_NAME", "zerodha")
            broker = BrokerGateway.from_name(broker_name)

            order_tracker = OrderTracker()
            dispatcher = DataDispatcher()
            dispatcher.register_main_queue(Queue())

            logger.info(f"Initializing strategy: {strategy_id}")
            strategy = strategy_class(broker, config, order_tracker)

            # Run SL reconciliation at startup
            if hasattr(strategy, "reconcile_sl_at_startup"):
                logger.info("=" * 60)
                logger.info("REVIEW & START STRATEGY - SL RECONCILIATION")
                logger.info("=" * 60)
                logger.info(
                    "Calculating SL levels for existing positions before placing orders..."
                )

                reconciliation_result = strategy.reconcile_sl_at_startup()

                if reconciliation_result:
                    logger.info(f"SL Reconciliation Complete:")
                    logger.info(
                        f"  - Total positions found: {reconciliation_result.total_positions}"
                    )
                    logger.info(
                        f"  - New SL orders placed: {reconciliation_result.new_sl_placed}"
                    )
                    logger.info(f"  - Errors: {len(reconciliation_result.errors)}")

                    state_queue.put(
                        {
                            "type": "SL_RECONCILIATION",
                            "total_positions": reconciliation_result.total_positions,
                            "new_sl_placed": reconciliation_result.new_sl_placed,
                            "errors": reconciliation_result.errors,
                        }
                    )
                else:
                    logger.info(
                        "SL reconciliation skipped (not enabled or not applicable)"
                    )

            # Initialize positions from broker for profit tracking
            enable_position_init = config.get("enable_position_init", True)
            if (
                hasattr(strategy, "initialize_positions_from_broker")
                and enable_position_init
            ):
                logger.info("=" * 60)
                logger.info("POSITION INITIALIZATION FROM BROKER")
                logger.info("=" * 60)
                logger.info(
                    "Loading existing positions from broker for profit target tracking..."
                )

                try:
                    initialized_count = strategy.initialize_positions_from_broker()

                    if initialized_count > 0:
                        logger.info(f"Position Initialization Complete:")
                        logger.info(f"  - Positions initialized: {initialized_count}")
                        logger.info(
                            f"  - Total positions now tracked: {len(strategy.positions)}"
                        )

                        state_queue.put(
                            {
                                "type": "POSITION_INIT",
                                "initialized_count": initialized_count,
                                "total_tracked": len(strategy.positions),
                            }
                        )
                    else:
                        logger.info("No existing positions found in broker account")
                except Exception as e:
                    logger.error(
                        f"Error initializing positions from broker: {e}", exc_info=True
                    )
            elif not enable_position_init:
                logger.info(
                    "Position initialization disabled (enable_position_init=false)"
                )

            # Check profit targets at startup
            if (
                hasattr(strategy, "check_profit_targets_at_startup")
                and enable_position_init
            ):
                logger.info("=" * 60)
                logger.info("PROFIT TARGET CHECK AT STARTUP")
                logger.info("=" * 60)
                logger.info(
                    "Checking if any existing positions have hit profit targets..."
                )

                try:
                    profit_check_result = strategy.check_profit_targets_at_startup()

                    if profit_check_result and len(profit_check_result) > 0:
                        logger.info(f"Profit Target Check Complete:")
                        logger.info(f"  - Positions exited: {len(profit_check_result)}")

                        state_queue.put(
                            {
                                "type": "PROFIT_CHECK",
                                "positions_exited": len(profit_check_result),
                            }
                        )
                    else:
                        logger.info("No positions hit profit target at startup")
                except Exception as e:
                    logger.error(
                        f"Error checking profit targets at startup: {e}", exc_info=True
                    )

            # Perform gap risk assessment at startup
            try:
                from strategy import GapRiskManager, PreMarketDataService
                from datetime import datetime
                
                logger.info("=" * 60)
                logger.info("GAP RISK ASSESSMENT")
                logger.info("=" * 60)
                
                gap_risk_manager = GapRiskManager()
                pre_market_service = PreMarketDataService()
                
                # Fetch pre-market data
                premarket_data = pre_market_service.fetch_all_data()
                
                # Calculate overnight gap if we have current price
                overnight_gap = 0.0
                try:
                    nifty_quote = broker.get_quote("NSE:NIFTY 50")
                    if nifty_quote and premarket_data.nifty_previous_close > 0:
                        overnight_gap = ((nifty_quote.last_price - premarket_data.nifty_previous_close)
                                        / premarket_data.nifty_previous_close) * 100
                        logger.info(f"Overnight gap: {overnight_gap:+.2f}%")
                except Exception as e:
                    logger.warning(f"Could not calculate overnight gap: {e}")
                
                # Assess gap risk
                assessment = gap_risk_manager.assess_gap_risk(
                    overnight_gap_percent=overnight_gap,
                    gift_nifty_gap=premarket_data.gift_nifty_gap_percent if premarket_data.gift_nifty_price else None
                )
                
                logger.info(f"Gap risk assessment: {assessment.message}")
                logger.info(f"Position multiplier: {assessment.position_multiplier:.0%}")
                logger.info(f"Can trade: {assessment.can_trade}")
                
                # Send gap risk notification
                state_queue.put(
                    {
                        "type": "GAP_RISK_UPDATE",
                        "gap_percent": overnight_gap,
                        "gift_nifty_gap": premarket_data.gift_nifty_gap_percent if premarket_data.gift_nifty_price else None,
                        "position_multiplier": assessment.position_multiplier,
                        "trading_blocked": not assessment.can_trade,
                        "reason": assessment.message,
                    }
                )
                
                # Check if Monday entry delay applies
                day_name = datetime.now().strftime("%A")
                current_time = datetime.now().time()
                if day_name == "Monday" and current_time.hour < 10:
                    wait_minutes = (10 - current_time.hour) * 60 - current_time.minute
                    logger.warning(f"Monday entry delay: Waiting {wait_minutes} minutes until 10:00 AM")
                    
            except Exception as e:
                logger.warning(f"Gap risk assessment failed: {e}")
                # Don't block trading if gap risk check fails

            # Send initial state
            state_queue.put(
                {
                    "type": "STATE_UPDATE",
                    "nifty_pe_last_value": getattr(strategy, "nifty_pe_last_value", 0),
                    "nifty_ce_last_value": getattr(strategy, "nifty_ce_last_value", 0),
                    "pe_reset_flag": getattr(strategy, "pe_reset_gap_flag", False),
                    "ce_reset_flag": getattr(strategy, "ce_reset_gap_flag", False),
                }
            )

            # WebSocket callbacks
            def on_ticks(ws, ticks):
                if isinstance(ticks, list):
                    dispatcher.dispatch(ticks)
                else:
                    if "symbol" in ticks:
                        dispatcher.dispatch(ticks)

            def on_connect(ws, response):
                logger.info(f"WebSocket connected: {response}")

            def on_order_update(ws, data):
                logger.info(f"Order update: {data}")

            # Connect WebSocket
            broker.connect_websocket(on_ticks=on_ticks, on_connect=on_connect)
            broker.symbols_to_subscribe([config.get("index_symbol", "NSE:NIFTY 50")])
            broker.connect_order_websocket(on_order_update=on_order_update)

            logger.info("Strategy started, entering main loop")

            # Main loop
            while True:
                try:
                    tick_data = dispatcher._main_queue.get(timeout=1)

                    if isinstance(tick_data, list):
                        symbol_data = tick_data[0]
                    else:
                        symbol_data = tick_data

                    if isinstance(symbol_data, dict) and (
                        "last_price" in symbol_data or "ltp" in symbol_data
                    ):
                        strategy.on_ticks_update(symbol_data)

                        state_queue.put(
                            {
                                "type": "STATE_UPDATE",
                                "nifty_pe_last_value": getattr(
                                    strategy, "nifty_pe_last_value", 0
                                ),
                                "nifty_ce_last_value": getattr(
                                    strategy, "nifty_ce_last_value", 0
                                ),
                                "pe_reset_flag": getattr(
                                    strategy, "pe_reset_gap_flag", False
                                ),
                                "ce_reset_flag": getattr(
                                    strategy, "ce_reset_gap_flag", False
                                ),
                            }
                        )

                except Exception as e:
                    if "empty" not in str(e).lower():
                        logger.error(f"Error in main loop: {e}")
                        state_queue.put({"type": "ERROR", "message": str(e)})

        except Exception as e:
            error_msg = f"Strategy process error: {str(e)}"
            print(error_msg)
            traceback.print_exc()
            state_queue.put({"type": "ERROR", "message": error_msg})


# Global strategy manager instance
strategy_manager = StrategyManager()
