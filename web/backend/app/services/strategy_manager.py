"""
Strategy Manager Service

Manages the lifecycle of the Survivor trading strategy.
Handles starting, stopping, and monitoring the strategy process.
"""
import asyncio
import multiprocessing
import os
import sys
import time
import yaml
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

# Add project root to Python path for imports
# Path structure: web/backend/app/services/ -> need to go up 4 levels to project root
_project_root = Path(__file__).resolve().parents[4]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from app.config import StrategyConfig, settings
from app.models.schemas import StrategyState, StrategyStatus


class StrategyManager:
    """
    Manages the Survivor trading strategy lifecycle.
    
    This service handles:
    - Starting and stopping the strategy
    - Monitoring strategy health
    - Managing strategy state
    - Configuration updates
    """
    
    _instance: Optional['StrategyManager'] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        self._initialized = True
        self._process: Optional[multiprocessing.Process] = None
        self._status = StrategyStatus.STOPPED
        self._start_time: Optional[float] = None
        self._error_message: Optional[str] = None
        
        # Strategy state (updated via shared memory or queues)
        self._nifty_pe_last_value: Optional[float] = None
        self._nifty_ce_last_value: Optional[float] = None
        self._pe_reset_flag: bool = False
        self._ce_reset_flag: bool = False
        self._last_update: Optional[datetime] = None
        
        # Configuration
        self._config: Optional[StrategyConfig] = None
        self._load_config()
        
        # State update queue for inter-process communication
        self._state_queue: Optional[multiprocessing.Queue] = None
        
    def _load_config(self) -> StrategyConfig:
        """Load configuration from YAML file."""
        config_path = settings.STRATEGY_CONFIG_PATH
        
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                config_data = yaml.safe_load(f)
                if config_data and 'default' in config_data:
                    self._config = StrategyConfig(**config_data['default'])
        else:
            self._config = StrategyConfig()
            
        return self._config
    
    def get_config(self) -> StrategyConfig:
        """Get current configuration."""
        if self._config is None:
            self._load_config()
        return self._config
    
    def update_config(self, updates: Dict[str, Any]) -> StrategyConfig:
        """
        Update configuration with new values.
        
        Args:
            updates: Dictionary of configuration updates
            
        Returns:
            Updated configuration
        """
        if self._config is None:
            self._load_config()
            
        # Update config values
        for key, value in updates.items():
            if value is not None and hasattr(self._config, key):
                setattr(self._config, key, value)
        
        # Save to file
        self._save_config()
        
        return self._config
    
    def _save_config(self) -> None:
        """Save current configuration to YAML file."""
        config_path = settings.STRATEGY_CONFIG_PATH
        
        config_data = {'default': self._config.model_dump()}
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        
        with open(config_path, 'w') as f:
            yaml.dump(config_data, f, default_flow_style=False)
    
    def get_state(self) -> StrategyState:
        """Get current strategy state."""
        uptime = None
        if self._start_time and self._status == StrategyStatus.RUNNING:
            uptime = time.time() - self._start_time
            
        return StrategyState(
            status=self._status,
            nifty_pe_last_value=self._nifty_pe_last_value,
            nifty_ce_last_value=self._nifty_ce_last_value,
            pe_reset_flag=self._pe_reset_flag,
            ce_reset_flag=self._ce_reset_flag,
            last_update=self._last_update,
            error_message=self._error_message,
            uptime_seconds=uptime
        )
    
    def start(self, config_override: Optional[Dict[str, Any]] = None) -> bool:
        """
        Start the trading strategy.
        
        Args:
            config_override: Optional configuration overrides
            
        Returns:
            True if started successfully
        """
        if self._status == StrategyStatus.RUNNING:
            return True
            
        if self._status == StrategyStatus.STARTING:
            return False
            
        try:
            self._status = StrategyStatus.STARTING
            self._error_message = None
            
            # Apply config overrides if provided
            if config_override:
                self.update_config(config_override)
            
            # Create state queue for inter-process communication
            self._state_queue = multiprocessing.Queue()
            
            # Start strategy process
            self._process = multiprocessing.Process(
                target=self._run_strategy_process,
                args=(self._state_queue,)
            )
            self._process.start()
            
            self._start_time = time.time()
            self._status = StrategyStatus.RUNNING
            
            # Start state monitor task
            asyncio.create_task(self._monitor_state())
            
            return True
            
        except Exception as e:
            self._status = StrategyStatus.ERROR
            self._error_message = str(e)
            return False
    
    def stop(self) -> bool:
        """
        Stop the trading strategy.
        
        Returns:
            True if stopped successfully
        """
        if self._status == StrategyStatus.STOPPED:
            return True
            
        if self._status == StrategyStatus.STOPPING:
            return False
            
        try:
            self._status = StrategyStatus.STOPPING
            
            if self._process and self._process.is_alive():
                # Terminate the process
                self._process.terminate()
                self._process.join(timeout=10)
                
                # Force kill if still alive
                if self._process.is_alive():
                    self._process.kill()
                    self._process.join()
            
            self._process = None
            self._status = StrategyStatus.STOPPED
            self._start_time = None
            
            return True
            
        except Exception as e:
            self._status = StrategyStatus.ERROR
            self._error_message = str(e)
            return False
    
    def restart(self, config_override: Optional[Dict[str, Any]] = None) -> bool:
        """
        Restart the trading strategy.
        
        Args:
            config_override: Optional configuration overrides
            
        Returns:
            True if restarted successfully
        """
        self.stop()
        time.sleep(1)  # Brief pause
        return self.start(config_override)
    
    async def _monitor_state(self) -> None:
        """Monitor strategy state updates from the process."""
        while self._status == StrategyStatus.RUNNING:
            try:
                if self._state_queue and not self._state_queue.empty():
                    state_update = self._state_queue.get_nowait()
                    self._process_state_update(state_update)
                    
                await asyncio.sleep(0.1)
                
            except Exception as e:
                print(f"Error monitoring state: {e}")
                await asyncio.sleep(1)
    
    def _process_state_update(self, update: Dict[str, Any]) -> None:
        """Process a state update from the strategy."""
        if update.get('type') == 'STATE_UPDATE':
            self._nifty_pe_last_value = update.get('nifty_pe_last_value')
            self._nifty_ce_last_value = update.get('nifty_ce_last_value')
            self._pe_reset_flag = update.get('pe_reset_flag', False)
            self._ce_reset_flag = update.get('ce_reset_flag', False)
            self._last_update = datetime.utcnow()
            
        elif update.get('type') == 'ERROR':
            error_message = update.get('message', 'Unknown error')
            self._error_message = error_message
            self._status = StrategyStatus.ERROR
            
            # Check for insufficient funds or margin errors
            insufficient_fund_keywords = [
                'insufficient',
                'margin',
                'funds',
                'balance',
                'exposure',
                'limit exceeded',
                'not enough',
                'margin shortfall'
            ]
            
            error_lower = error_message.lower()
            is_insufficient_funds = any(keyword in error_lower for keyword in insufficient_fund_keywords)
            
            if is_insufficient_funds:
                print(f"[StrategyManager] Insufficient funds detected: {error_message}")
                print("[StrategyManager] Stopping strategy due to insufficient funds...")
                # Stop the strategy process
                self._stop_on_error(f"INSUFFICIENT FUNDS: {error_message}")
    
    def _stop_on_error(self, error_message: str) -> None:
        """Stop the strategy due to a critical error."""
        try:
            if self._process and self._process.is_alive():
                self._process.terminate()
                self._process.join(timeout=5)
                
                if self._process.is_alive():
                    self._process.kill()
                    self._process.join()
            
            self._process = None
            self._status = StrategyStatus.ERROR
            self._error_message = error_message
            self._start_time = None
            
        except Exception as e:
            print(f"[StrategyManager] Error stopping strategy: {e}")
    
    @staticmethod
    def _run_strategy_process(state_queue: multiprocessing.Queue) -> None:
        """
        Run the strategy in a separate process.
        
        This is the target function for the multiprocessing Process.
        """
        try:
            # Import strategy components
            from strategy.survivor_enhanced import EnhancedSurvivorStrategy
            from brokers import BrokerGateway
            from dispatcher import DataDispatcher
            from orders import OrderTracker
            from queue import Queue
            
            # Set up timestamped logging for this strategy run
            from logger import setup_strategy_logging_with_name
            logger = setup_strategy_logging_with_name("enhanced")
            logger.info("=" * 70)
            logger.info("STARTING ENHANCED SURVIVOR STRATEGY")
            logger.info("=" * 70)
            
            # Load config
            config_path = settings.STRATEGY_CONFIG_PATH
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)['default']
            
            # Initialize broker
            broker = BrokerGateway.from_name(os.getenv("BROKER_NAME"))
            
            # Initialize components
            order_tracker = OrderTracker()
            dispatcher = DataDispatcher()
            dispatcher.register_main_queue(Queue())
            
            # Initialize strategy (Enhanced version with SL management)
            strategy = EnhancedSurvivorStrategy(broker, config, order_tracker)
            
            # Run SL reconciliation at startup
            if hasattr(strategy, 'reconcile_sl_at_startup'):
                logger.info("=" * 60)
                logger.info("RUNNING SL RECONCILIATION FOR EXISTING POSITIONS")
                logger.info("=" * 60)
                result = strategy.reconcile_sl_at_startup()
                if result:
                    logger.info(f"SL Reconciliation: {result.new_sl_placed} new SL orders placed "
                               f"for {result.total_positions} positions")
            
            # Send initial state
            state_queue.put({
                'type': 'STATE_UPDATE',
                'nifty_pe_last_value': strategy.nifty_pe_last_value,
                'nifty_ce_last_value': strategy.nifty_ce_last_value,
                'pe_reset_flag': strategy.pe_reset_gap_flag,
                'ce_reset_flag': strategy.ce_reset_gap_flag
            })
            
            # Set up websocket callbacks
            def on_ticks(ws, ticks):
                if isinstance(ticks, list):
                    dispatcher.dispatch(ticks)
                else:
                    if "symbol" in ticks:
                        dispatcher.dispatch(ticks)
                        
            def on_connect(ws, response):
                logger.info(f"Websocket connected: {response}")
                
            def on_order_update(ws, data):
                logger.info(f"Order update: {data}")
            
            # Assign callbacks
            broker.on_ticks = on_ticks
            broker.on_connect = on_connect
            broker.on_order_update = on_order_update
            
            # Connect websocket
            broker.connect_websocket(on_ticks=on_ticks, on_connect=on_connect)
            broker.symbols_to_subscribe([config['index_symbol']])
            broker.connect_order_websocket(on_order_update=on_order_update)
            
            time.sleep(5)  # Wait for connection
            
            # Main trading loop
            while True:
                try:
                    tick_data = dispatcher._main_queue.get()
                    
                    if isinstance(tick_data, list):
                        symbol_data = tick_data[0]
                    else:
                        symbol_data = tick_data
                        
                    if isinstance(symbol_data, dict) and ('last_price' in symbol_data or 'ltp' in symbol_data):
                        strategy.on_ticks_update(symbol_data)
                        
                        # Send state update
                        state_queue.put({
                            'type': 'STATE_UPDATE',
                            'nifty_pe_last_value': strategy.nifty_pe_last_value,
                            'nifty_ce_last_value': strategy.nifty_ce_last_value,
                            'pe_reset_flag': strategy.pe_reset_gap_flag,
                            'ce_reset_flag': strategy.ce_reset_gap_flag
                        })
                        
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    logger.error(f"Error in strategy loop: {e}")
                    state_queue.put({
                        'type': 'ERROR',
                        'message': str(e)
                    })
                    
        except Exception as e:
            state_queue.put({
                'type': 'ERROR',
                'message': str(e)
            })


# Global strategy manager instance
strategy_manager = StrategyManager()
