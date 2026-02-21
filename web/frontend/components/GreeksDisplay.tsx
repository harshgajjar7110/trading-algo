'use client';

import { useState, useEffect, useCallback } from 'react';
import { GreeksResponse, PositionGreeks, PortfolioGreeks } from '@/types';
import { greeksAPI } from '@/lib/api';
import { formatCurrency, formatNumber } from '@/lib/utils';

interface GreeksDisplayProps {
  refreshInterval?: number; // in milliseconds
}

export default function GreeksDisplay({ refreshInterval = 30000 }: GreeksDisplayProps) {
  const [greeks, setGreeks] = useState<GreeksResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showDetails, setShowDetails] = useState(false);

  const fetchGreeks = useCallback(async () => {
    try {
      const data = await greeksAPI.getPortfolioGreeks();
      setGreeks(data);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch Greeks');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchGreeks();
    const interval = setInterval(fetchGreeks, refreshInterval);
    return () => clearInterval(interval);
  }, [fetchGreeks, refreshInterval]);

  if (loading) {
    return (
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <div className="animate-pulse">
          <div className="h-4 bg-gray-200 rounded w-1/4 mb-4"></div>
          <div className="grid grid-cols-5 gap-4">
            {[...Array(5)].map((_, i) => (
              <div key={i} className="h-16 bg-gray-100 rounded"></div>
            ))}
          </div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <h3 className="text-lg font-medium text-gray-900 mb-4">Portfolio Greeks</h3>
        <div className="text-sm text-gray-500">
          {error.includes('No open positions') 
            ? 'No positions to calculate Greeks. Open positions will show risk metrics here.'
            : `Error loading Greeks: ${error}`
          }
        </div>
      </div>
    );
  }

  if (!greeks) return null;

  const { portfolio, interpretation, positions } = greeks;

  const getGreekColor = (value: number, type: 'delta' | 'gamma' | 'theta' | 'vega' | 'rho') => {
    const absValue = Math.abs(value);
    switch (type) {
      case 'delta':
        return absValue > 100 ? 'text-red-600' : absValue > 50 ? 'text-yellow-600' : 'text-green-600';
      case 'gamma':
        return absValue > 0.5 ? 'text-red-600' : absValue > 0.1 ? 'text-yellow-600' : 'text-green-600';
      case 'theta':
        return value > 0 ? 'text-green-600' : value < -50 ? 'text-red-600' : 'text-yellow-600';
      case 'vega':
        return absValue > 500 ? 'text-red-600' : absValue > 200 ? 'text-yellow-600' : 'text-green-600';
      case 'rho':
        return absValue > 100 ? 'text-yellow-600' : 'text-gray-600';
      default:
        return 'text-gray-900';
    }
  };

  return (
    <div className="bg-white rounded-xl shadow-sm border border-gray-200">
      <div className="px-6 py-4 border-b border-gray-200 flex justify-between items-center">
        <div>
          <h3 className="text-lg font-medium text-gray-900">Portfolio Greeks</h3>
          <p className="text-sm text-gray-500">
            Risk metrics @ NIFTY {formatNumber(greeks.underlying_price)} • {new Date(greeks.calculation_date).toLocaleDateString()}
          </p>
        </div>
        <button
          onClick={() => setShowDetails(!showDetails)}
          className="text-sm text-primary-600 hover:text-primary-700 font-medium"
        >
          {showDetails ? 'Hide Details' : 'Show Details'}
        </button>
      </div>

      {/* Portfolio Summary */}
      <div className="p-6">
        <div className="grid grid-cols-5 gap-4">
          {/* Delta */}
          <div className="text-center p-4 bg-gray-50 rounded-lg">
            <div className="text-sm text-gray-500 mb-1">Delta</div>
            <div className={`text-2xl font-bold ${getGreekColor(portfolio.delta, 'delta')}`}>
              {formatNumber(portfolio.delta, 2)}
            </div>
            <div className="text-xs text-gray-400 mt-1">
              {portfolio.delta > 0 ? 'Bullish' : portfolio.delta < 0 ? 'Bearish' : 'Neutral'}
            </div>
          </div>

          {/* Gamma */}
          <div className="text-center p-4 bg-gray-50 rounded-lg">
            <div className="text-sm text-gray-500 mb-1">Gamma</div>
            <div className={`text-2xl font-bold ${getGreekColor(portfolio.gamma, 'gamma')}`}>
              {formatNumber(portfolio.gamma, 4)}
            </div>
            <div className="text-xs text-gray-400 mt-1">
              {portfolio.gamma > 0 ? 'Long Gamma' : portfolio.gamma < 0 ? 'Short Gamma' : 'Flat'}
            </div>
          </div>

          {/* Theta */}
          <div className="text-center p-4 bg-gray-50 rounded-lg">
            <div className="text-sm text-gray-500 mb-1">Theta (Daily)</div>
            <div className={`text-2xl font-bold ${getGreekColor(portfolio.theta, 'theta')}`}>
              {formatCurrency(portfolio.theta)}
            </div>
            <div className="text-xs text-gray-400 mt-1">
              {portfolio.theta > 0 ? 'Time Decay +' : 'Time Decay -'}
            </div>
          </div>

          {/* Vega */}
          <div className="text-center p-4 bg-gray-50 rounded-lg">
            <div className="text-sm text-gray-500 mb-1">Vega</div>
            <div className={`text-2xl font-bold ${getGreekColor(portfolio.vega, 'vega')}`}>
              {formatNumber(portfolio.vega, 1)}
            </div>
            <div className="text-xs text-gray-400 mt-1">
              {portfolio.vega > 0 ? 'Long Vol' : portfolio.vega < 0 ? 'Short Vol' : 'Neutral'}
            </div>
          </div>

          {/* Rho */}
          <div className="text-center p-4 bg-gray-50 rounded-lg">
            <div className="text-sm text-gray-500 mb-1">Rho</div>
            <div className={`text-2xl font-bold ${getGreekColor(portfolio.rho, 'rho')}`}>
              {formatNumber(portfolio.rho, 1)}
            </div>
            <div className="text-xs text-gray-400 mt-1">Rate Sensitivity</div>
          </div>
        </div>

        {/* Interpretations */}
        <div className="mt-6 space-y-2">
          <h4 className="text-sm font-medium text-gray-700">Risk Interpretation</h4>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <div className="flex items-start space-x-2 text-sm">
              <span className="w-2 h-2 rounded-full bg-blue-500 mt-1.5 flex-shrink-0"></span>
              <span className="text-gray-600">{interpretation.delta}</span>
            </div>
            <div className="flex items-start space-x-2 text-sm">
              <span className="w-2 h-2 rounded-full bg-purple-500 mt-1.5 flex-shrink-0"></span>
              <span className="text-gray-600">{interpretation.gamma}</span>
            </div>
            <div className="flex items-start space-x-2 text-sm">
              <span className="w-2 h-2 rounded-full bg-green-500 mt-1.5 flex-shrink-0"></span>
              <span className="text-gray-600">{interpretation.theta}</span>
            </div>
            <div className="flex items-start space-x-2 text-sm">
              <span className="w-2 h-2 rounded-full bg-orange-500 mt-1.5 flex-shrink-0"></span>
              <span className="text-gray-600">{interpretation.vega}</span>
            </div>
          </div>
        </div>

        {/* Position Details */}
        {showDetails && positions.length > 0 && (
          <div className="mt-6">
            <h4 className="text-sm font-medium text-gray-700 mb-3">Position Breakdown by Expiry</h4>
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead>
                  <tr className="text-xs font-medium text-gray-500 uppercase tracking-wider">
                    <th className="px-3 py-2 text-left">Symbol</th>
                    <th className="px-3 py-2 text-center">Qty</th>
                    <th className="px-3 py-2 text-center">Strike</th>
                    <th className="px-3 py-2 text-center">Expiry</th>
                    <th className="px-3 py-2 text-right">Delta</th>
                    <th className="px-3 py-2 text-right">Gamma</th>
                    <th className="px-3 py-2 text-right">Theta</th>
                    <th className="px-3 py-2 text-right">Vega</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200 text-sm">
                  {positions.map((pos, idx) => (
                    <tr key={idx} className="hover:bg-gray-50">
                      <td className="px-3 py-2 font-medium">
                        {pos.symbol}
                        <span className={`ml-2 text-xs ${pos.option_type === 'CE' ? 'text-green-600' : 'text-red-600'}`}>
                          {pos.option_type}
                        </span>
                      </td>
                      <td className="px-3 py-2 text-center">{pos.quantity}</td>
                      <td className="px-3 py-2 text-center">{pos.strike}</td>
                      <td className="px-3 py-2 text-center text-xs text-gray-500">
                        {pos.expiry_date ? new Date(pos.expiry_date).toLocaleDateString('en-IN', { 
                          day: 'numeric', 
                          month: 'short' 
                        }) : 'N/A'}
                      </td>
                      <td className={`px-3 py-2 text-right ${getGreekColor(pos.delta, 'delta')}`}>
                        {formatNumber(pos.delta, 2)}
                      </td>
                      <td className={`px-3 py-2 text-right ${getGreekColor(pos.gamma, 'gamma')}`}>
                        {formatNumber(pos.gamma, 4)}
                      </td>
                      <td className={`px-3 py-2 text-right ${getGreekColor(pos.theta, 'theta')}`}>
                        {formatCurrency(pos.theta)}
                      </td>
                      <td className={`px-3 py-2 text-right ${getGreekColor(pos.vega, 'vega')}`}>
                        {formatNumber(pos.vega, 1)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* Legend */}
        <div className="mt-6 pt-4 border-t border-gray-100">
          <div className="grid grid-cols-2 md:grid-cols-5 gap-2 text-xs text-gray-400">
            <div>
              <span className="font-medium">Delta:</span> Price sensitivity
            </div>
            <div>
              <span className="font-medium">Gamma:</span> Delta change rate
            </div>
            <div>
              <span className="font-medium">Theta:</span> Daily time decay
            </div>
            <div>
              <span className="font-medium">Vega:</span> Volatility sensitivity
            </div>
            <div>
              <span className="font-medium">Rho:</span> Rate sensitivity
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
