"""Test registered routes"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'web', 'backend'))

from app.main import app

print('Registered Routes:')
print('=' * 80)

for route in app.routes:
    if hasattr(route, 'methods') and hasattr(route, 'path'):
        methods = ','.join(route.methods - {'HEAD'})
        print(f'{methods:10} {route.path}')
    elif hasattr(route, 'routes'):
        # Router
        for subroute in route.routes:
            if hasattr(subroute, 'methods') and hasattr(subroute, 'path'):
                methods = ','.join(subroute.methods - {'HEAD'})
                print(f'{methods:10} {subroute.path}')

print('=' * 80)
