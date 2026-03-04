"""
Memory monitoring utilities for tracking and optimizing memory usage.
"""

import gc
import logging
from typing import Optional, Dict, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Try to import tracemalloc for detailed tracking
try:
    import tracemalloc
    TRACEMALLOC_AVAILABLE = True
except ImportError:
    TRACEMALLOC_AVAILABLE = False

# Try to import psutil for process memory tracking
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False


@dataclass
class MemoryStats:
    """Memory statistics snapshot."""
    rss_mb: float  # Resident Set Size in MB
    vms_mb: float  # Virtual Memory Size in MB
    tracemalloc_current_mb: Optional[float] = None
    tracemalloc_peak_mb: Optional[float] = None
    gc_objects: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging."""
        return {
            'rss_mb': round(self.rss_mb, 2),
            'vms_mb': round(self.vms_mb, 2),
            'tracemalloc_current_mb': round(self.tracemalloc_current_mb, 2) if self.tracemalloc_current_mb else None,
            'tracemalloc_peak_mb': round(self.tracemalloc_peak_mb, 2) if self.tracemalloc_peak_mb else None,
            'gc_objects': self.gc_objects,
        }


class MemoryMonitor:
    """
    Monitor memory usage of the trading strategy.
    
    Usage:
        monitor = MemoryMonitor()
        monitor.start()
        
        # ... run strategy ...
        
        stats = monitor.get_stats()
        logger.info(f"Memory usage: {stats.rss_mb} MB")
        
        monitor.stop()
    """
    
    def __init__(self, name: str = "strategy"):
        self.name = name
        self._started = False
        self._process = None
        
        if PSUTIL_AVAILABLE:
            import psutil
            self._process = psutil.Process()
    
    def start(self) -> None:
        """Start memory monitoring."""
        if TRACEMALLOC_AVAILABLE:
            tracemalloc.start()
            logger.info(f"[{self.name}] Tracemalloc started")
        
        self._started = True
        
        # Log initial state
        stats = self.get_stats()
        logger.info(f"[{self.name}] Memory monitoring started: {stats.to_dict()}")
    
    def stop(self) -> None:
        """Stop memory monitoring."""
        if TRACEMALLOC_AVAILABLE and self._started:
            tracemalloc.stop()
            logger.info(f"[{self.name}] Tracemalloc stopped")
        
        self._started = False
    
    def get_stats(self) -> MemoryStats:
        """Get current memory statistics."""
        rss_mb = 0.0
        vms_mb = 0.0
        
        if PSUTIL_AVAILABLE and self._process:
            mem_info = self._process.memory_info()
            rss_mb = mem_info.rss / 1024 / 1024
            vms_mb = mem_info.vms / 1024 / 1024
        
        tracemalloc_current = None
        tracemalloc_peak = None
        
        if TRACEMALLOC_AVAILABLE and self._started:
            current, peak = tracemalloc.get_traced_memory()
            tracemalloc_current = current / 1024 / 1024
            tracemalloc_peak = peak / 1024 / 1024
        
        gc_objects = len(gc.get_objects())
        
        return MemoryStats(
            rss_mb=rss_mb,
            vms_mb=vms_mb,
            tracemalloc_current_mb=tracemalloc_current,
            tracemalloc_peak_mb=tracemalloc_peak,
            gc_objects=gc_objects
        )
    
    def log_stats(self, context: str = "") -> None:
        """Log current memory statistics."""
        stats = self.get_stats()
        prefix = f"[{self.name}]"
        if context:
            prefix += f" {context}:"
        logger.info(f"{prefix} Memory stats: {stats.to_dict()}")
    
    def force_gc(self) -> int:
        """Force garbage collection and return number of collected objects."""
        collected = gc.collect()
        logger.debug(f"[{self.name}] Garbage collection: {collected} objects collected")
        return collected
    
    def get_top_allocations(self, limit: int = 10) -> Optional[list]:
        """Get top memory allocations (requires tracemalloc)."""
        if not TRACEMALLOC_AVAILABLE or not self._started:
            return None
        
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics('lineno')[:limit]
        
        return [
            {
                'file': stat.traceback.format()[-1] if stat.traceback else 'unknown',
                'size_mb': round(stat.size / 1024 / 1024, 2),
                'count': stat.count
            }
            for stat in top_stats
        ]


def format_bytes(size_bytes: int) -> str:
    """Format bytes to human readable string."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"


def get_object_size(obj: Any) -> int:
    """Get approximate memory size of an object."""
    import sys
    return sys.getsizeof(obj)
