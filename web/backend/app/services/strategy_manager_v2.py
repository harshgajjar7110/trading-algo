"""
Strategy Manager V2

Enhanced strategy manager that supports multiple strategy types
with selection, configuration, and confirmation flow.
"""
import asyncio
import multiprocessing
import os
import sys
import time
import yaml
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Callable

# Add project root to Python path
_project_root = Path(__file__).resolve().parents[4]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from app.config import settings
from app.models.schemas import StrategyState, StrategyStatus
from app.services.strategy_registry import (
    StrategyRegistry, StrategyInstance, StrategyType
)


class StrategyManagerV2:
    """
    Enhanced Strategy Manager with multi-strategy support.
    
    Features:
    - Strategy selection from registered strategies
    - Configuration validation and confirmation
    - Per-strategy config management
    - Clear identification of running strategy
    """
    
    _instance: Optional['StrategyManagerV2'] = None
    
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
        
        # Callbacks for state changes
        self._on_state_change: Optional[Callable] = None
        
    def get_available_strategies(self) -> Dict:
        """Get list of available strategies for selection"""
        return {
            "strategies": self._registry.get_strategy_comparison(),
            "current": self.get_current_strategy_info()
        }
    
    def get_strategy_details(self, strategy_id: str) -> Optional[Dict]:
        """Get detailed info about a specific strategy"""
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
    
    def preview_strategy_config(self, strategy_id: str, config_override: Optional[Dict] = None) -> Dict:
        """
        Preview the configuration that will be used when starting a strategy.
        This allows users to confirm parameters before starting.
        """
        info = self._registry.get(strategy_id)
        if not info:
            return {"error": f"Strategy not found: {strategy_id}"}
        
        # Load base config
        base_config = self._registry.load_config(strategy_id)
        
        # Merge with overrides
        preview_config = base_config.copy()
        if config_override:
            preview_config.update(config_override)
        
        # Validate
        errors = self._registry.validate_config(strategy_id, preview_config)
        
        # Show key differences from defaults
        differences = []
        for key, value in preview_config.items():
            if key in info.default_params and info.default_params[key] != value:
                differences.append({
                    "param": key,
                    "default": info.default_params[key],
                    "current": value
                })
        
        # Extract SL configuration for display
        sl_config = {
            "enabled": preview_config.get('sl_enabled', False),
            "percentage": preview_config.get('sl_percentage', 60),
            "order_type": preview_config.get('sl_order_type', 'STOP_LIMIT'),
            "limit_buffer": preview_config.get('sl_limit_buffer', 5.0),
            "reconcile_on_start": preview_config.get('sl_reconcile_on_start', True),
        }
        
        # Calculate potential SL levels for display
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
        """
        Calculate SL preview information based on configuration.
        Shows what SL prices would be for typical entry prices.
        """
        sl_percentage = config.get('sl_percentage', 60)
        sl_limit_buffer = config.get('sl_limit_buffer', 5.0)
        sl_enabled = config.get('sl_enabled', False)
        
        if not sl_enabled:
            return {
                "enabled": False,
                "message": "SL orders are disabled",
            }
        
        # Example calculations for different entry prices (for short positions)
        examples = []
        for entry_price in [50.0, 100.0, 150.0, 200.0]:
            trigger_price = round(entry_price * (1 + sl_percentage / 100), 2)
            limit_price = round(trigger_price + sl_limit_buffer, 2)
            examples.append({
                "entry_price": entry_price,
                "trigger_price": trigger_price,
                "limit_price": limit_price,
                "potential_loss_pct": sl_percentage,
            })
        
        return {
            "enabled": True,
            "sl_percentage": sl_percentage,
            "sl_limit_buffer": sl_limit_buffer,
            "example_calculations": examples,
            "message": f"SL will be placed at {sl_percentage}% above entry price for short positions",
        }
    
    def start(self, strategy_id: str, config_override: Optional[Dict] = None, confirmed: bool = False) -> Dict:
        """
        Start a strategy with the given ID.
        
        Args:
            strategy_id: The strategy to start
            config_override: Optional config overrides
            confirmed: Whether user has confirmed the config
            
        Returns:
            Dict with status and message
        """
        # Check if already running
        if self._status == StrategyStatus.RUNNING:
            current_id = self._registry.get_current_strategy_id()
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
        
        try:
            # Validate strategy exists
            info = self._registry.get(strategy_id)
            if not info:
                return {
                    "success": False,
                    "message": f"Unknown strategy: {strategy_id}",
                    "requires_confirmation": False,
                }
            
            # Create instance (loads and validates config)
            instance = self._registry.create_instance(strategy_id, config_override)
            
            # If not confirmed, return preview for confirmation
            if not confirmed:
                preview = self.preview_strategy_config(strategy_id, config_override)
                return {
                    "success": False,
                    "message": "Confirmation required",
                    "requires_confirmation": True,
                    "preview": preview,
                }
            
            # User confirmed - start the strategy
            self._status = StrategyStatus.STARTING
            self._error_message = None
            
            # Set as current instance
            self._current_instance = instance
            self._registry.set_current_instance(instance)
            
            # Create communication queue
            self._state_queue = multiprocessing.Queue()
            
            # Start strategy process
            self._process = multiprocessing.Process(
                target=self._run_strategy_process,
                args=(strategy_id, instance.config, self._state_queue)
            )
            self._process.start()
            
            self._start_time = time.time()
            self._status = StrategyStatus.RUNNING
            instance.status = "RUNNING"
            instance.started_at = datetime.utcnow()
            instance.pid = self._process.pid
            
            # Start state monitor
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
        """Stop the currently running strategy"""
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
            
            # Update instance
            if self._current_instance:
                self._current_instance.status = "STOPPED"
                self._current_instance.stopped_at = datetime.utcnow()
            
            self._cleanup()
            
            return {
                "success": True,
                "message": "Strategy stopped successfully"
            }
            
        except Exception as e:
            self._status = StrategyStatus.ERROR
            self._error_message = str(e)
            return {
                "success": False,
                "message": f"Error stopping: {str(e)}"
            }
    
    def restart(self, strategy_id: Optional[str] = None, config_override: Optional[Dict] = None, confirmed: bool = False) -> Dict:
        """Restart strategy (optionally with different strategy/config)"""
        stop_result = self.stop()
        if not stop_result["success"]:
            return stop_result
        
        time.sleep(1)
        
        # Determine which strategy to use
        if strategy_id is None:
            if self._current_instance:
                strategy_id = self._current_instance.strategy_type.value
                logger.info(f"Restarting with current strategy: {strategy_id}")
            else:
                strategy_id = "enhanced_survivor"
                logger.info(f"No current strategy instance, defaulting to: {strategy_id}")
        
        return self.start(strategy_id, config_override, confirmed)
    
    def get_state(self) -> StrategyState:
        """Get current strategy state"""
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
            # Extended fields
            current_strategy=current_strategy,
            instance_id=self._current_instance.id if self._current_instance else None,
        )
    
    def get_current_strategy_info(self) -> Optional[Dict]:
        """Get info about currently running strategy"""
        if not self._current_instance:
            return None
        
        return {
            "id": self._current_instance.id,
            "strategy_id": self._current_instance.strategy_type.value,
            "name": self._current_instance.name,
            "status": self._current_instance.status,
            "started_at": self._current_instance.started_at.isoformat() if self._current_instance.started_at else None,
            "pid": self._current_instance.pid,
            "config_summary": {
                k: v for k, v in self._current_instance.config.items()
                if k in ['symbol_initials', 'pe_gap', 'ce_gap', 'pe_quantity', 'ce_quantity', 'entry_filter_type']
            }
        }
    
    def _cleanup(self):
        """Clean up resources"""
        self._process = None
        self._state_queue = None
        self._status = StrategyStatus.STOPPED
        self._start_time = None
        self._registry.clear_current_instance()
    
    async def _monitor_state(self):
        """Monitor state updates from strategy process"""
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
        """Process state update from strategy"""
        if update.get('type') == 'STATE_UPDATE':
            self._nifty_pe_last_value = update.get('nifty_pe_last_value')
            self._nifty_ce_last_value = update.get('nifty_ce_last_value')
            self._pe_reset_flag = update.get('pe_reset_flag', False)
            self._ce_reset_flag = update.get('ce_reset_flag', False)
            self._last_update = datetime.utcnow()
            
        elif update.get('type') == 'ERROR':
            error_msg = update.get('message', 'Unknown error')
            self._error_message = error_msg
            self._status = StrategyStatus.ERROR
            
            if self._current_instance:
                self._current_instance.status = "ERROR"
                self._current_instance.error_message = error_msg
            
            # Auto-stop on critical errors
            if any(kw in error_msg.lower() for kw in ['insufficient', 'margin', 'funds']):
                self._stop_on_error(f"INSUFFICIENT FUNDS: {error_msg}")
    
    def _stop_on_error(self, error_message: str):
        """Stop strategy due to error"""
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
    
    @staticmethod
    def _run_strategy_process(strategy_id: str, config: Dict, state_queue: multiprocessing.Queue):
        """Run strategy in separate process"""
        try:
            # Import here to avoid pickling issues
            from brokers import BrokerGateway
            from dispatcher import DataDispatcher
            from orders import OrderTracker
            from queue import Queue
            from logger import setup_strategy_logging_with_name
            
            # Set up timestamped logging for this strategy run
            logger = setup_strategy_logging_with_name(strategy_id)
            logger.info("=" * 70)
            logger.info(f"STARTING STRATEGY: {strategy_id}")
            logger.info("=" * 70)
            
            # Load strategy class dynamically
            registry = StrategyRegistry()
            strategy_class = registry.load_strategy_class(strategy_id)
            
            # Initialize broker
            broker_name = os.getenv("BROKER_NAME", "zerodha")
            broker = BrokerGateway.from_name(broker_name)
            
            # Initialize components
            order_tracker = OrderTracker()
            dispatcher = DataDispatcher()
            dispatcher.register_main_queue(Queue())
            
            # Initialize strategy
            logger.info(f"Initializing strategy: {strategy_id}")
            strategy = strategy_class(broker, config, order_tracker)
            
            # Run SL reconciliation at startup (for enhanced strategy)
            if hasattr(strategy, 'reconcile_sl_at_startup'):
                logger.info("=" * 60)
                logger.info("REVIEW & START STRATEGY - SL RECONCILIATION")
                logger.info("=" * 60)
                logger.info("Calculating SL levels for existing positions before placing orders...")
                
                reconciliation_result = strategy.reconcile_sl_at_startup()
                
                if reconciliation_result:
                    logger.info(f"SL Reconciliation Complete:")
                    logger.info(f"  - Total positions found: {reconciliation_result.total_positions}")
                    logger.info(f"  - New SL orders placed: {reconciliation_result.new_sl_placed}")
                    logger.info(f"  - Errors: {len(reconciliation_result.errors)}")
                    
                    # Send reconciliation info to parent process
                    state_queue.put({
                        'type': 'SL_RECONCILIATION',
                        'total_positions': reconciliation_result.total_positions,
                        'new_sl_placed': reconciliation_result.new_sl_placed,
                        'errors': reconciliation_result.errors,
                    })
                else:
                    logger.info("SL reconciliation skipped (not enabled or not applicable)")
            
            # Send initial state
            state_queue.put({
                'type': 'STATE_UPDATE',
                'nifty_pe_last_value': getattr(strategy, 'nifty_pe_last_value', None),
                'nifty_ce_last_value': getattr(strategy, 'nifty_ce_last_value', None),
                'pe_reset_flag': getattr(strategy, 'pe_reset_gap_flag', False),
                'ce_reset_flag': getattr(strategy, 'ce_reset_gap_flag', False),
            })
            
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
            broker.symbols_to_subscribe([config.get('index_symbol', 'NSE:NIFTY 50')])
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
                    
                    if isinstance(symbol_data, dict) and ('last_price' in symbol_data or 'ltp' in symbol_data):
                        strategy.on_ticks_update(symbol_data)
                        
                        # Send state update
                        state_queue.put({
                            'type': 'STATE_UPDATE',
                            'nifty_pe_last_value': getattr(strategy, 'nifty_pe_last_value', None),
                            'nifty_ce_last_value': getattr(strategy, 'nifty_ce_last_value', None),
                            'pe_reset_flag': getattr(strategy, 'pe_reset_gap_flag', False),
                            'ce_reset_flag': getattr(strategy, 'ce_reset_gap_flag', False),
                        })
                        
                except Exception as e:
                    if "empty" not in str(e).lower():
                        logger.error(f"Error in main loop: {e}")
                        state_queue.put({
                            'type': 'ERROR',
                            'message': str(e)
                        })
                        
        except Exception as e:
            error_msg = f"Strategy process error: {str(e)}"
            print(error_msg)
            traceback.print_exc()
            state_queue.put({
                'type': 'ERROR',
                'message': error_msg
            })


# Global instance
strategy_manager_v2 = StrategyManagerV2()
