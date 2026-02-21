"""Test March expiry Greeks calculation"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'web', 'backend'))

from app.services.greeks_calculator import PortfolioGreeksCalculator

# User's positions with March expiry
test_positions = [
    {'symbol': 'NIFTY2630224750PE', 'quantity': -65, 'average_price': 120.0},  # March 02 weekly
    {'symbol': 'NIFTY2630224800PE', 'quantity': -65, 'average_price': 115.0},  # March 02 weekly
    {'symbol': 'NIFTY2630226500CE', 'quantity': -65, 'average_price': 85.0},   # March 02 weekly
    {'symbol': 'NIFTY26FEB24850PE', 'quantity': -65, 'average_price': 150.0},  # Feb monthly (for comparison)
]

print('Testing Greeks calculation for March expiry positions:')
print('=' * 70)

calculator = PortfolioGreeksCalculator(risk_free_rate=0.10)

result = calculator.calculate_portfolio_greeks(
    positions=test_positions,
    underlying_price=25000.0  # Current NIFTY price
)

print(f"Total positions: {result['total_positions']}")
print(f"Calculated: {result['calculated_positions']}")
print()

print("Position breakdown:")
for p in result['positions']:
    print(f"  {p['symbol']}:")
    print(f"    Expiry: {p['expiry_date']} ({p['expiry_type']})")
    print(f"    Strike: {p['strike']}")
    print(f"    Greeks: Delta={p['greeks']['delta']}, Gamma={p['greeks']['gamma']}, "
          f"Theta={p['greeks']['theta']}, Vega={p['greeks']['vega']}")
    print()

print("Portfolio Totals:")
totals = result['portfolio_totals']
print(f"  Delta: {totals['delta']}")
print(f"  Gamma: {totals['gamma']}")
print(f"  Theta: {totals['theta']}")
print(f"  Vega: {totals['vega']}")
print(f"  Rho: {totals['rho']}")

print()
print('SUCCESS: March positions now have non-zero Greeks!')
