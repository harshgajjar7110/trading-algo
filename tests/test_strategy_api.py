"""Test strategy API"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'web', 'backend'))

from app.services.strategy_manager_v2 import strategy_manager_v2

result = strategy_manager_v2.get_available_strategies()
print('Strategies found:', len(result['strategies']))
for s in result['strategies']:
    print(f"  - {s['id']}: {s['name']}")
