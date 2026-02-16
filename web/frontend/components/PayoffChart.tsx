'use client';

import { useState, useEffect, useCallback } from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ReferenceLine,
  ResponsiveContainer,
  Area,
  ComposedChart,
} from 'recharts';
import { PortfolioPayoff, PositionPayoff, OptionType } from '@/types';

interface PayoffChartProps {
  className?: string;
}

export default function PayoffChart({ className = '' }: PayoffChartProps) {
  const [payoffData, setPayoffData] = useState<PortfolioPayoff | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [viewMode, setViewMode] = useState<'chart' | 'table' | 'both'>('both');
  const [selectedPositions, setSelectedPositions] = useState<Set<string>>(new Set());

  const fetchPayoffData = useCallback(async () => {
    try {
      const response = await fetch('http://localhost:8000/api/analysis/payoff');
      if (!response.ok) {
        if (response.status === 404) {
          setError('No open positions found');
        } else {
          throw new Error('Failed to fetch payoff data');
        }
        return;
      }
      const data: PortfolioPayoff = await response.json();
      setPayoffData(data);
      // Select all positions by default
      setSelectedPositions(new Set(data.positions.map(p => p.symbol)));
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load payoff analysis');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchPayoffData();
    // Refresh every 30 seconds
    const interval = setInterval(fetchPayoffData, 30000);
    return () => clearInterval(interval);
  }, [fetchPayoffData]);

  const togglePosition = (symbol: string) => {
    const newSelected = new Set(selectedPositions);
    if (newSelected.has(symbol)) {
      newSelected.delete(symbol);
    } else {
      newSelected.add(symbol);
    }
    setSelectedPositions(newSelected);
  };

  const formatCurrency = (value: number | null) => {
    if (value === null) return 'Unlimited';
    return `₹${value.toLocaleString('en-IN', { maximumFractionDigits: 2 })}`;
  };

  const formatPrice = (value: number) => {
    return value.toLocaleString('en-IN', { maximumFractionDigits: 0 });
  };

  // Prepare chart data - combine selected positions
  const prepareChartData = () => {
    if (!payoffData) return [];

    // Create a map of price -> P&L for each selected position
    const priceMap = new Map<number, { price: number; combined: number; [key: string]: number }>();

    // Initialize with combined curve prices
    payoffData.combined_payoff_curve.forEach(point => {
      priceMap.set(point.underlying_price, { 
        price: point.underlying_price, 
        combined: 0 
      });
    });

    // Add individual position P&Ls
    payoffData.positions.forEach(pos => {
      if (!selectedPositions.has(pos.symbol)) return;
      pos.payoff_curve.forEach(point => {
        const entry = priceMap.get(point.underlying_price);
        if (entry) {
          entry[pos.symbol] = point.pnl;
          entry.combined += point.pnl;
        }
      });
    });

    return Array.from(priceMap.values()).sort((a, b) => a.price - b.price);
  };

  // Generate colors for positions
  const getPositionColor = (index: number) => {
    const colors = [
      '#3b82f6', // blue
      '#ef4444', // red
      '#10b981', // green
      '#f59e0b', // amber
      '#8b5cf6', // violet
      '#ec4899', // pink
      '#06b6d4', // cyan
      '#84cc16', // lime
    ];
    return colors[index % colors.length];
  };

  if (loading) {
    return (
      <div className={`bg-white rounded-lg shadow p-6 ${className}`}>
        <div className="animate-pulse">
          <div className="h-6 bg-gray-200 rounded w-1/4 mb-4"></div>
          <div className="h-64 bg-gray-200 rounded"></div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className={`bg-white rounded-lg shadow p-6 ${className}`}>
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Payoff Analysis</h3>
        <div className="text-center py-8 text-gray-500">
          <svg className="mx-auto h-12 w-12 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
          </svg>
          <p className="mt-2">{error}</p>
          <button
            onClick={fetchPayoffData}
            className="mt-4 px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  if (!payoffData) return null;

  const chartData = prepareChartData();

  return (
    <div className={`bg-white rounded-lg shadow ${className}`}>
      {/* Header */}
      <div className="p-4 border-b border-gray-200">
        <div className="flex items-center justify-between">
          <div>
            <h3 className="text-lg font-semibold text-gray-900">Payoff Analysis</h3>
            <p className="text-sm text-gray-500">
              NIFTY at {formatPrice(payoffData.current_nifty_price)} • 
              Range: {formatPrice(payoffData.price_range_start)} - {formatPrice(payoffData.price_range_end)}
            </p>
          </div>
          <div className="flex items-center gap-2">
            <select
              value={viewMode}
              onChange={(e) => setViewMode(e.target.value as 'chart' | 'table' | 'both')}
              className="px-3 py-1.5 border border-gray-300 rounded text-sm"
            >
              <option value="both">Chart & Table</option>
              <option value="chart">Chart Only</option>
              <option value="table">Table Only</option>
            </select>
            <button
              onClick={fetchPayoffData}
              className="px-3 py-1.5 bg-gray-100 text-gray-700 rounded hover:bg-gray-200 text-sm"
            >
              Refresh
            </button>
          </div>
        </div>
      </div>

      {/* Summary Cards */}
      <div className="p-4 grid grid-cols-2 md:grid-cols-5 gap-4 bg-gray-50">
        <div className="bg-white p-3 rounded-lg">
          <p className="text-xs text-gray-500 uppercase">Current P&L</p>
          <p className={`text-lg font-semibold ${payoffData.total_current_pnl >= 0 ? 'text-green-600' : 'text-red-600'}`}>
            {formatCurrency(payoffData.total_current_pnl)}
          </p>
        </div>
        <div className="bg-white p-3 rounded-lg">
          <p className="text-xs text-gray-500 uppercase">Max Profit</p>
          <p className="text-lg font-semibold text-green-600">
            {formatCurrency(payoffData.total_max_profit)}
          </p>
        </div>
        <div className="bg-white p-3 rounded-lg">
          <p className="text-xs text-gray-500 uppercase">Max Loss</p>
          <p className={`text-lg font-semibold ${payoffData.total_max_loss === null ? 'text-red-600' : 'text-orange-600'}`}>
            {formatCurrency(payoffData.total_max_loss)}
          </p>
        </div>
        <div className="bg-white p-3 rounded-lg">
          <p className="text-xs text-gray-500 uppercase">Positions</p>
          <p className="text-lg font-semibold text-gray-900">
            {payoffData.positions.length}
          </p>
        </div>
        <div className="bg-white p-3 rounded-lg">
          <p className="text-xs text-gray-500 uppercase">Combined Breakeven</p>
          <p className="text-sm font-semibold text-blue-600">
            {payoffData.combined_breakeven_points.length > 0 
              ? payoffData.combined_breakeven_points.map(be => formatPrice(be)).join(', ')
              : 'N/A'
            }
          </p>
        </div>
      </div>

      {/* Position Toggles */}
      <div className="px-4 py-2 border-b border-gray-200 flex flex-wrap gap-2">
        {payoffData.positions.map((pos, idx) => (
          <button
            key={pos.symbol}
            onClick={() => togglePosition(pos.symbol)}
            className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
              selectedPositions.has(pos.symbol)
                ? 'text-white'
                : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
            }`}
            style={{
              backgroundColor: selectedPositions.has(pos.symbol) ? getPositionColor(idx) : undefined,
            }}
          >
            {pos.symbol} ({pos.side})
          </button>
        ))}
      </div>

      {/* Chart */}
      {(viewMode === 'chart' || viewMode === 'both') && (
        <div className="p-4">
          <div className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <ComposedChart data={chartData} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis
                  dataKey="price"
                  tickFormatter={formatPrice}
                  stroke="#6b7280"
                  fontSize={12}
                />
                <YAxis
                  tickFormatter={(v) => `₹${(v / 1000).toFixed(0)}k`}
                  stroke="#6b7280"
                  fontSize={12}
                />
                <Tooltip
                  formatter={(value: number) => formatCurrency(value)}
                  labelFormatter={(label) => `NIFTY: ${formatPrice(label as number)}`}
                  contentStyle={{ backgroundColor: 'white', border: '1px solid #e5e7eb' }}
                />
                <Legend />
                
                {/* Zero line */}
                <ReferenceLine y={0} stroke="#374151" strokeWidth={1} />
                
                {/* Current NIFTY price line */}
                <ReferenceLine
                  x={payoffData.current_nifty_price}
                  stroke="#3b82f6"
                  strokeDasharray="5 5"
                  label={{ value: 'Current', position: 'top', fill: '#3b82f6', fontSize: 10 }}
                />

                {/* Individual position lines */}
                {payoffData.positions.map((pos, idx) => {
                  if (!selectedPositions.has(pos.symbol)) return null;
                  return (
                    <Line
                      key={pos.symbol}
                      type="monotone"
                      dataKey={pos.symbol}
                      stroke={getPositionColor(idx)}
                      strokeWidth={2}
                      dot={false}
                      name={pos.symbol}
                    />
                  );
                })}

                {/* Combined P&L area */}
                {selectedPositions.size > 1 && (
                  <Area
                    type="monotone"
                    dataKey="combined"
                    fill="#3b82f6"
                    fillOpacity={0.1}
                    stroke="#3b82f6"
                    strokeWidth={2}
                    name="Combined P&L"
                  />
                )}
              </ComposedChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {/* Table */}
      {(viewMode === 'table' || viewMode === 'both') && (
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Symbol</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Type</th>
                <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Strike</th>
                <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Qty</th>
                <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Premium</th>
                <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Breakeven</th>
                <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">BE %</th>
                <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Max Profit</th>
                <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Max Loss</th>
                <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Current P&L</th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {payoffData.positions.map((pos, idx) => (
                <tr key={pos.symbol} className="hover:bg-gray-50">
                  <td className="px-4 py-3 whitespace-nowrap">
                    <div className="flex items-center">
                      <div
                        className="w-2 h-2 rounded-full mr-2"
                        style={{ backgroundColor: getPositionColor(idx) }}
                      />
                      <span className="text-sm font-medium text-gray-900">{pos.symbol}</span>
                    </div>
                  </td>
                  <td className="px-4 py-3 whitespace-nowrap">
                    <span className={`inline-flex px-2 py-1 text-xs font-medium rounded ${
                      pos.option_type === OptionType.CALL
                        ? 'bg-green-100 text-green-800'
                        : 'bg-red-100 text-red-800'
                    }`}>
                      {pos.option_type} {pos.side}
                    </span>
                  </td>
                  <td className="px-4 py-3 whitespace-nowrap text-sm text-right text-gray-900">
                    {formatPrice(pos.strike_price)}
                  </td>
                  <td className="px-4 py-3 whitespace-nowrap text-sm text-right text-gray-900">
                    {pos.quantity}
                  </td>
                  <td className="px-4 py-3 whitespace-nowrap text-sm text-right text-gray-900">
                    ₹{pos.premium.toFixed(2)}
                  </td>
                  <td className="px-4 py-3 whitespace-nowrap text-sm text-right text-gray-900">
                    {formatPrice(pos.breakeven)}
                  </td>
                  <td className={`px-4 py-3 whitespace-nowrap text-sm text-right font-medium ${
                    pos.breakeven_percent >= 0 ? 'text-green-600' : 'text-red-600'
                  }`}>
                    {pos.breakeven_percent >= 0 ? '+' : ''}{pos.breakeven_percent.toFixed(2)}%
                  </td>
                  <td className="px-4 py-3 whitespace-nowrap text-sm text-right text-green-600 font-medium">
                    {formatCurrency(pos.max_profit)}
                  </td>
                  <td className="px-4 py-3 whitespace-nowrap text-sm text-right text-red-600 font-medium">
                    {formatCurrency(pos.max_loss)}
                  </td>
                  <td className={`px-4 py-3 whitespace-nowrap text-sm text-right font-medium ${
                    pos.current_pnl >= 0 ? 'text-green-600' : 'text-red-600'
                  }`}>
                    {formatCurrency(pos.current_pnl)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* P&L at Different Prices */}
      {viewMode === 'both' && (
        <div className="p-4 border-t border-gray-200">
          <h4 className="text-sm font-medium text-gray-700 mb-3">P&L at Different NIFTY Levels</h4>
          <div className="grid grid-cols-2 md:grid-cols-5 gap-2">
            {[-0.05, -0.02, 0, 0.02, 0.05].map(pct => {
              const targetPrice = Math.round(payoffData.current_nifty_price * (1 + pct));
              const combinedPnl = chartData.find(d => d.price === targetPrice)?.combined || 0;
              return (
                <div key={pct} className="bg-gray-50 p-2 rounded text-center">
                  <p className="text-xs text-gray-500">
                    {pct >= 0 ? '+' : ''}{(pct * 100).toFixed(0)}%
                  </p>
                  <p className="text-sm font-medium">{formatPrice(targetPrice)}</p>
                  <p className={`text-sm font-semibold ${combinedPnl >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                    {formatCurrency(combinedPnl)}
                  </p>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}
