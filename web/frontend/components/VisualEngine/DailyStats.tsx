/**
 * DailyStats Component
 * 
 * Displays daily trading statistics including:
 * - Trades taken/rejected
 * - P&L
 * - Consecutive losses
 */

'use client';

import React from 'react';
import { clsx } from 'clsx';
import type { DailyStats as DailyStatsType } from '@/types';

interface DailyStatsProps {
  stats: DailyStatsType;
}

// SVG Icons
const TrendingUpIcon = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <polyline points="23 6 13.5 15.5 8.5 10.5 1 18" />
    <polyline points="17 6 23 6 23 12" />
  </svg>
);

const TrendingDownIcon = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <polyline points="23 18 13.5 8.5 8.5 13.5 1 6" />
    <polyline points="17 18 23 18 23 12" />
  </svg>
);

const ActivityIcon = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <polyline points="22 12 18 12 15 21 9 3 6 12 2 12" />
  </svg>
);

const BanIcon = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <circle cx="12" cy="12" r="10" />
    <line x1="4.93" y1="4.93" x2="19.07" y2="19.07" />
  </svg>
);

const AlertTriangleIcon = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z" />
    <line x1="12" y1="9" x2="12" y2="13" />
    <line x1="12" y1="17" x2="12.01" y2="17" />
  </svg>
);

interface StatCardProps {
  label: string;
  value: string | number;
  icon: React.ReactNode;
  trend?: 'up' | 'down' | 'neutral';
  color?: 'green' | 'red' | 'blue' | 'amber' | 'gray';
  subtitle?: string;
}

const StatCard: React.FC<StatCardProps> = ({ 
  label, 
  value, 
  icon, 
  trend = 'neutral',
  color = 'gray',
  subtitle
}) => {
  const colorClasses = {
    green: {
      bg: 'bg-emerald-50',
      text: 'text-emerald-600',
      border: 'border-emerald-200',
      iconBg: 'bg-emerald-100'
    },
    red: {
      bg: 'bg-rose-50',
      text: 'text-rose-600',
      border: 'border-rose-200',
      iconBg: 'bg-rose-100'
    },
    blue: {
      bg: 'bg-blue-50',
      text: 'text-blue-600',
      border: 'border-blue-200',
      iconBg: 'bg-blue-100'
    },
    amber: {
      bg: 'bg-amber-50',
      text: 'text-amber-600',
      border: 'border-amber-200',
      iconBg: 'bg-amber-100'
    },
    gray: {
      bg: 'bg-gray-50',
      text: 'text-gray-600',
      border: 'border-gray-200',
      iconBg: 'bg-gray-100'
    }
  };

  const colors = colorClasses[color];

  return (
    <div className={clsx(
      "relative rounded-xl border p-4 transition-all duration-200",
      colors.bg,
      colors.border
    )}>
      <div className="flex items-start justify-between">
        <div>
          <p className="text-xs font-medium text-gray-500 uppercase tracking-wide">
            {label}
          </p>
          <p className={clsx(
            "text-2xl font-bold mt-1",
            colors.text
          )}>
            {value}
          </p>
          {subtitle && (
            <p className="text-xs text-gray-400 mt-1">
              {subtitle}
            </p>
          )}
        </div>
        <div className={clsx(
          "p-2.5 rounded-lg",
          colors.iconBg,
          colors.text
        )}>
          {icon}
        </div>
      </div>
      
      {/* Trend indicator */}
      {trend !== 'neutral' && (
        <div className={clsx(
          "absolute bottom-3 right-4 flex items-center gap-1 text-xs font-medium",
          trend === 'up' ? 'text-emerald-600' : 'text-rose-600'
        )}>
          {trend === 'up' ? (
            <>
              <TrendingUpIcon className="h-3.5 w-3.5" />
              <span>+</span>
            </>
          ) : (
            <>
              <TrendingDownIcon className="h-3.5 w-3.5" />
              <span>-</span>
            </>
          )}
        </div>
      )}
    </div>
  );
};

export const DailyStats: React.FC<DailyStatsProps> = ({ stats }) => {
  const totalAttempts = stats.trades_taken + stats.trades_rejected;
  const acceptanceRate = totalAttempts > 0 
    ? ((stats.trades_taken / totalAttempts) * 100).toFixed(1)
    : '0.0';
  
  const isProfit = stats.pnl > 0;
  const isLoss = stats.pnl < 0;
  const pnlColor = isProfit ? 'green' : isLoss ? 'red' : 'gray';
  
  // Warning for consecutive losses
  const showLossWarning = stats.consecutive_losses >= 2;

  return (
    <div className="rounded-xl border bg-white shadow-sm p-5">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h3 className="text-sm font-medium text-gray-500">Daily Statistics</h3>
          <p className="text-xs text-gray-400 mt-0.5">
            Trading session performance
          </p>
        </div>
        
        {/* Date badge */}
        <span className="px-3 py-1 text-xs font-medium rounded-full bg-gray-100 text-gray-600">
          {new Date().toLocaleDateString('en-IN', { 
            weekday: 'short', 
            day: 'numeric', 
            month: 'short' 
          })}
        </span>
      </div>

      {/* Main stats grid */}
      <div className="grid grid-cols-2 gap-3 mb-3">
        <StatCard
          label="P&L"
          value={`₹${stats.pnl.toFixed(2)}`}
          icon={<ActivityIcon className="h-5 w-5" />}
          color={pnlColor}
          trend={isProfit ? 'up' : isLoss ? 'down' : 'neutral'}
        />
        
        <StatCard
          label="Trades Taken"
          value={stats.trades_taken}
          icon={<TrendingUpIcon className="h-5 w-5" />}
          color="blue"
          subtitle={`${acceptanceRate}% acceptance rate`}
        />
      </div>

      {/* Secondary stats */}
      <div className="grid grid-cols-2 gap-3">
        <StatCard
          label="Trades Rejected"
          value={stats.trades_rejected}
          icon={<BanIcon className="h-5 w-5" />}
          color="amber"
          subtitle="By filters"
        />
        
        <StatCard
          label="Consecutive Losses"
          value={stats.consecutive_losses}
          icon={<AlertTriangleIcon className="h-5 w-5" />}
          color={showLossWarning ? 'red' : 'gray'}
          subtitle={showLossWarning ? 'Warning!' : 'Within limits'}
        />
      </div>

      {/* Warning banner for consecutive losses */}
      {showLossWarning && (
        <div className="mt-4 p-3 rounded-lg bg-rose-50 border border-rose-200">
          <div className="flex items-start gap-2">
            <AlertTriangleIcon className="h-4 w-4 text-rose-500 flex-shrink-0 mt-0.5" />
            <div>
              <p className="text-sm font-medium text-rose-700">
                High Consecutive Losses
              </p>
              <p className="text-xs text-rose-600 mt-0.5">
                {stats.consecutive_losses} consecutive losses detected. 
                Consider reviewing strategy or taking a break.
              </p>
            </div>
          </div>
        </div>
      )}

      {/* Summary text */}
      <div className="mt-4 pt-4 border-t border-gray-100">
        <p className="text-xs text-gray-400 text-center">
          {stats.trades_taken === 0 
            ? 'No trades taken yet today'
            : stats.pnl > 0 
              ? `Profitable session: +₹${stats.pnl.toFixed(2)}`
              : stats.pnl < 0
                ? `Losing session: ₹${stats.pnl.toFixed(2)}`
                : 'Break-even session'
          }
        </p>
      </div>
    </div>
  );
};

export default DailyStats;
