"""
Strategy Registry

Manages available trading strategies and their configurations.
Provides a registry pattern for loading and running different strategies.
"""
import os
import yaml
from typing import Dict, List, Optional, Type, Any
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class StrategyType(str, Enum):
    """Available strategy types"""
    SURVIVOR = "survivor"
    ENHANCED_SURVIVOR = "enhanced_survivor"
    WAVE = "wave"


@dataclass
class StrategyInfo:
    """Metadata about a trading strategy"""
    id: str
    name: str
    description: str
    class_path: str  # e.g., "strategy.survivor:SurvivorStrategy"
    config_path: str  # Path to YAML config file
    version: str = "1.0.0"
    author: str = ""
    tags: List[str] = field(default_factory=list)
    
    # Risk profile
    risk_level: str = "medium"  # low, medium, high
    recommended_capital: str = ""
    
    # Default parameters (for UI display)
    default_params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StrategyInstance:
    """Represents a running or configured strategy instance"""
    id: str
    strategy_type: StrategyType
    name: str
    config: Dict[str, Any]
    status: str = "STOPPED"  # STOPPED, RUNNING, ERROR
    started_at: Optional[datetime] = None
    stopped_at: Optional[datetime] = None
    pid: Optional[int] = None
    error_message: Optional[str] = None


class StrategyRegistry:
    """
    Registry of available trading strategies.
    
    Manages:
    - Strategy metadata and discovery
    - Strategy class loading
    - Configuration management per strategy
    - Running instance tracking
    """
    
    _instance: Optional['StrategyRegistry'] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self._initialized = True
        self._strategies: Dict[str, StrategyInfo] = {}
        self._current_instance: Optional[StrategyInstance] = None
        self._register_default_strategies()
    
    def _register_default_strategies(self):
        """Register built-in strategies"""
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        
        # Enhanced Survivor Strategy (Default)
        self.register(StrategyInfo(
            id="enhanced_survivor",
            name="Enhanced Survivor Strategy",
            description="""
            Advanced options selling with technical filters (RSI, EMA, ADX),
            dynamic gaps, position limits, and stop-loss management.
            Better for volatile markets.
            """,
            class_path="strategy.survivor_enhanced:EnhancedSurvivorStrategy",
            config_path=os.path.join(project_root, "strategy", "configs", "survivor_enhanced.yml"),
            version="2.0.0",
            tags=["options", "selling", "technical-indicators", "risk-management"],
            risk_level="medium-high",
            recommended_capital="₹10L+",
            default_params={
                "pe_gap": 40,
                "ce_gap": 40,
                "pe_symbol_gap": 800,
                "ce_symbol_gap": 800,
                "pe_quantity": 65,
                "ce_quantity": 65,
                "entry_filter_type": "RSI",
                "rsi_min": 30,
                "rsi_max": 70,
                "max_positions_per_side": 3,
                "stop_loss_multiplier": 2.0,
                "sl_enabled": True,
                "sl_percentage": 60,
                "sl_reconcile_on_start": True,
            }
        ))
        
        # Survivor Strategy (Base)
        self.register(StrategyInfo(
            id="survivor",
            name="Survivor Strategy",
            description="""
            Classic gap-based options selling strategy. 
            Sells PE when NIFTY rises, CE when NIFTY falls.
            Simple and reliable for steady income.
            """,
            class_path="strategy.survivor:SurvivorStrategy",
            config_path=os.path.join(project_root, "strategy", "configs", "survivor.yml"),
            version="1.0.0",
            tags=["options", "selling", "gap-based", "nifty"],
            risk_level="medium",
            recommended_capital="₹5L+",
            default_params={
                "pe_gap": 40,
                "ce_gap": 40,
                "pe_symbol_gap": 800,
                "ce_symbol_gap": 800,
                "pe_quantity": 65,
                "ce_quantity": 65,
            }
        ))
        
        # Wave Strategy (if config exists)
        wave_config = os.path.join(project_root, "strategy", "configs", "wave.yml")
        if os.path.exists(wave_config):
            self.register(StrategyInfo(
                id="wave",
                name="Wave Strategy",
                description="""
                Trend-following strategy based on wave patterns.
                Buy on uptrends, sell on downtrends.
                """,
                class_path="strategy.wave:WaveStrategy",
                config_path=wave_config,
                version="1.0.0",
                tags=["trend-following", "waves", "momentum"],
                risk_level="high",
                recommended_capital="₹10L+",
            ))
    
    def register(self, strategy_info: StrategyInfo):
        """Register a new strategy"""
        self._strategies[strategy_info.id] = strategy_info
    
    def get(self, strategy_id: str) -> Optional[StrategyInfo]:
        """Get strategy info by ID"""
        return self._strategies.get(strategy_id)
    
    def list_strategies(self) -> List[StrategyInfo]:
        """List all registered strategies"""
        return list(self._strategies.values())
    
    def get_strategy_names(self) -> Dict[str, str]:
        """Get mapping of strategy IDs to display names"""
        return {s.id: s.name for s in self._strategies.values()}
    
    def load_strategy_class(self, strategy_id: str):
        """Dynamically load strategy class"""
        strategy_info = self.get(strategy_id)
        if not strategy_info:
            raise ValueError(f"Strategy not found: {strategy_id}")
        
        # Parse class path (e.g., "strategy.survivor:SurvivorStrategy")
        module_path, class_name = strategy_info.class_path.split(":")
        
        # Import module
        import importlib
        module = importlib.import_module(module_path)
        
        # Get class
        strategy_class = getattr(module, class_name)
        return strategy_class
    
    def load_config(self, strategy_id: str) -> Dict[str, Any]:
        """Load configuration for a strategy"""
        strategy_info = self.get(strategy_id)
        if not strategy_info:
            raise ValueError(f"Strategy not found: {strategy_id}")
        
        config_path = strategy_info.config_path
        if not os.path.exists(config_path):
            # Return default params if config file doesn't exist
            return strategy_info.default_params.copy()
        
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
            # Support both 'default' key and flat config
            if config_data and 'default' in config_data:
                return config_data['default']
            return config_data or {}
    
    def save_config(self, strategy_id: str, config: Dict[str, Any]):
        """Save configuration for a strategy"""
        strategy_info = self.get(strategy_id)
        if not strategy_info:
            raise ValueError(f"Strategy not found: {strategy_id}")
        
        config_path = strategy_info.config_path
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        
        # Wrap in 'default' key for compatibility
        config_data = {'default': config}
        
        with open(config_path, 'w') as f:
            yaml.dump(config_data, f, default_flow_style=False)
    
    def validate_config(self, strategy_id: str, config: Dict[str, Any]) -> List[str]:
        """Validate configuration for a strategy"""
        errors = []
        
        # Common validations
        if 'symbol_initials' in config:
            if not config['symbol_initials'] or len(config['symbol_initials']) < 5:
                errors.append("symbol_initials must be at least 5 characters")
        
        if 'pe_quantity' in config and 'ce_quantity' in config:
            if config['pe_quantity'] <= 0 or config['ce_quantity'] <= 0:
                errors.append("Quantities must be positive")
        
        if 'pe_gap' in config and 'ce_gap' in config:
            if config['pe_gap'] <= 0 or config['ce_gap'] <= 0:
                errors.append("Gaps must be positive")
        
        # Strategy-specific validations
        if strategy_id == "enhanced_survivor":
            filter_type = config.get('entry_filter_type', 'NONE')
            if filter_type not in ['NONE', 'RSI', 'EMA', 'ADX', 'ALL']:
                errors.append("entry_filter_type must be one of: NONE, RSI, EMA, ADX, ALL")
            
            if config.get('rsi_min', 0) >= config.get('rsi_max', 100):
                errors.append("rsi_min must be less than rsi_max")
        
        return errors
    
    def create_instance(self, strategy_id: str, config_override: Optional[Dict] = None) -> StrategyInstance:
        """Create a new strategy instance"""
        strategy_info = self.get(strategy_id)
        if not strategy_info:
            raise ValueError(f"Strategy not found: {strategy_id}")
        
        # Load base config
        base_config = self.load_config(strategy_id)
        
        # Apply overrides
        if config_override:
            base_config.update(config_override)
        
        # Validate
        errors = self.validate_config(strategy_id, base_config)
        if errors:
            raise ValueError(f"Config validation failed: {', '.join(errors)}")
        
        # Create instance
        instance = StrategyInstance(
            id=f"{strategy_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            strategy_type=StrategyType(strategy_id),
            name=strategy_info.name,
            config=base_config,
            status="STOPPED"
        )
        
        return instance
    
    def set_current_instance(self, instance: StrategyInstance):
        """Set the currently running/active strategy instance"""
        self._current_instance = instance
    
    def get_current_instance(self) -> Optional[StrategyInstance]:
        """Get the currently running strategy instance"""
        return self._current_instance
    
    def get_current_strategy_id(self) -> Optional[str]:
        """Get the ID of the currently running strategy"""
        if self._current_instance:
            return self._current_instance.strategy_type.value
        return None
    
    def clear_current_instance(self):
        """Clear the current strategy instance"""
        self._current_instance = None
    
    def get_strategy_comparison(self) -> List[Dict]:
        """Get comparison data for all strategies"""
        comparison = []
        for s in self._strategies.values():
            comparison.append({
                "id": s.id,
                "name": s.name,
                "risk_level": s.risk_level,
                "recommended_capital": s.recommended_capital,
                "description": s.description[:100] + "..." if len(s.description) > 100 else s.description,
                "tags": s.tags,
                "key_params": list(s.default_params.keys())[:5] if s.default_params else []
            })
        return comparison


# Global registry instance
strategy_registry = StrategyRegistry()
