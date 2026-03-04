/**
 * FilterPanel Component
 * 
 * Displays the four entry filters (RSI, EMA, ADX, Gap Risk) with their
 * current status and threshold values.
 */

'use client';

import React from 'react';
import { clsx } from 'clsx';
import type { FilterStatus } from '@/types';

interface FilterPanelProps {
  filters: FilterStatus[] | Record<string, FilterStatus>;
}

// Filter configuration with display names and descriptions
const FILTER_CONFIG: Record<string, { 
  name: string; 
  description: string;
  icon: React.ReactNode;
}> = {
  rsi: {
    name: 'RSI Filter',
    description: 'Overbought/Oversold detection',
    icon: (
      <svg className="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
        <path d="M3 3v18h18" />
        <path d="M7 16l4-4 4 4 5-5" />
      </svg>
    ),
  },
  ema: {
    name: 'EMA Filter',
    description: 'Trend direction alignment',
    icon: (
      <svg className="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
        <path d="M3 3v18h18" />
        <path d="M3 12c3-3 5 3 8-2s4 6 7 0 3-4 3-4" />
      </svg>
    ),
  },
  adx: {
    name: 'ADX Filter',
    description: 'Trend strength confirmation',
    icon: (
      <svg className="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
        <path d="M12 2v20M2 12h20" />
        <circle cx="12" cy="12" r="3" />
      </svg>
    ),
  },
  gap_risk: {
    name: 'Gap Risk',
    description: 'Market open volatility check',
    icon: (
      <svg className="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
        <path d="M12 2L2 7l10 5 10-5-10-5z" />
        <path d="M2 17l10 5 10-5" />
        <path d="M2 12l10 5 10-5" />
      </svg>
    ),
  },
};

// Get status color classes
const getStatusClasses = (status: string): { bg: string; text: string; border: string } => {
  switch (status.toLowerCase()) {
    case 'passed':
    case 'true':
    case 'ok':
      return {
        bg: 'bg-emerald-50',
        text: 'text-emerald-600',
        border: 'border-emerald-200',
      };
    case 'failed':
    case 'false':
    case 'blocked':
      return {
        bg: 'bg-red-50',
        text: 'text-red-600',
        border: 'border-red-200',
      };
    case 'pending':
    case 'waiting':
      return {
        bg: 'bg-amber-50',
        text: 'text-amber-600',
        border: 'border-amber-200',
      };
    default:
      return {
        bg: 'bg-gray-50',
        text: 'text-gray-600',
        border: 'border-gray-200',
      };
  }
};

// Format filter value for display
const formatValue = (filter: FilterStatus): string => {
  if (filter.current_value != null && filter.threshold != null) {
    return `${filter.current_value.toFixed(1)} / ${filter.threshold.toFixed(1)}`;
  }
  if (filter.current_value != null) {
    return filter.current_value.toFixed(1);
  }
  return filter.status;
};

// Get passed status from filter
const isFilterPassed = (filter: FilterStatus): boolean => {
  return filter.status.toLowerCase() === 'passed' ||
         filter.status.toLowerCase() === 'true' ||
         filter.status.toLowerCase() === 'ok';
};

const FilterCard: React.FC<{
  filterId: string;
  filter: FilterStatus;
}> = ({ filterId, filter }) => {
  const config = FILTER_CONFIG[filterId] || {
    name: filterId.toUpperCase(),
    description: 'Filter status',
    icon: (
      <svg className="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
        <circle cx="12" cy="12" r="10" />
        <line x1="12" y1="16" x2="12" y2="12" />
        <line x1="12" y1="8" x2="12.01" y2="8" />
      </svg>
    ),
  };

  const statusClasses = getStatusClasses(filter.status);
  const passed = isFilterPassed(filter);

  return (
    <div className={clsx(
      "relative rounded-lg border p-4 transition-all duration-200",
      "hover:shadow-sm",
      statusClasses.bg,
      statusClasses.border
    )}>
      {/* Status indicator dot */}
      <div className={clsx(
        "absolute top-3 right-3 h-2.5 w-2.5 rounded-full",
        passed ? "bg-emerald-500" : "bg-red-500"
      )} />

      <div className="flex items-start gap-3">
        <div className={clsx(
          "flex-shrink-0 p-2 rounded-lg",
          "bg-white/60",
          statusClasses.text
        )}>
          {config.icon}
        </div>

        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <h4 className={clsx(
              "font-semibold text-sm",
              statusClasses.text
            )}>
              {config.name}
            </h4>
          </div>

          <p className="text-xs text-gray-500 mt-0.5">
            {config.description}
          </p>

          <div className="mt-2 flex items-center justify-between">
            <span className={clsx(
              "text-lg font-mono font-semibold",
              statusClasses.text
            )}>
              {formatValue(filter)}
            </span>

            <span className={clsx(
              "px-2 py-0.5 text-xs font-medium rounded-full",
              "bg-white/60",
              statusClasses.text
            )}>
              {filter.status}
            </span>
          </div>
        </div>
      </div>

      {/* Additional details if available */}
      {filter.message && (
        <div className="mt-2 text-xs text-gray-500 italic">
          {filter.message}
        </div>
      )}
    </div>
  );
};

export const FilterPanel: React.FC<FilterPanelProps> = ({ filters }) => {
  // Convert array or record to entries
  const filterEntries: [string, FilterStatus][] = Array.isArray(filters)
    ? filters.map(f => [f.name.toLowerCase().replace(/\s+/g, '_'), f])
    : Object.entries(filters);

  return (
    <div className="rounded-xl border bg-white shadow-sm p-5">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-sm font-medium text-gray-500">Entry Filters</h3>
        <span className="text-xs text-gray-400">
          {filterEntries.filter(([, f]) => isFilterPassed(f)).length}/{filterEntries.length} Passed
        </span>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        {filterEntries.map(([id, filter]) => (
          <FilterCard key={id} filterId={id} filter={filter} />
        ))}
      </div>

      {filterEntries.length === 0 && (
        <div className="text-center py-8 text-gray-400">
          <svg className="h-12 w-12 mx-auto mb-3 text-gray-300" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5">
            <path d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" />
          </svg>
          <p className="text-sm">No filters configured</p>
        </div>
      )}
    </div>
  );
};

export default FilterPanel;
