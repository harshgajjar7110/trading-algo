'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import { authAPI } from '@/lib/api';
import { LoginUrlResponse, AuthStatusResponse, TokenVerifyResponse } from '@/types';

export default function AuthPage() {
  const [status, setStatus] = useState<AuthStatusResponse | null>(null);
  const [verifyResult, setVerifyResult] = useState<TokenVerifyResponse | null>(null);
  const [loginUrl, setLoginUrl] = useState<LoginUrlResponse | null>(null);
  const [requestToken, setRequestToken] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  // Fetch auth status on mount
  useEffect(() => {
    fetchStatus();
  }, []);

  const fetchStatus = async () => {
    try {
      const statusData = await authAPI.getStatus();
      setStatus(statusData);
      
      // If authenticated, verify the token works
      if (statusData.authenticated) {
        const verifyData = await authAPI.verifyToken();
        setVerifyResult(verifyData);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch auth status');
    }
  };

  const handleGetLoginUrl = async () => {
    try {
      setLoading(true);
      setError(null);
      const response = await authAPI.getLoginUrl();
      setLoginUrl(response);
      if (!response.success) {
        setError(response.message);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to get login URL');
    } finally {
      setLoading(false);
    }
  };

  const handleSubmitToken = async () => {
    if (!requestToken.trim()) {
      setError('Please enter the request token');
      return;
    }

    try {
      setLoading(true);
      setError(null);
      const response = await authAPI.submitToken(requestToken.trim());
      
      if (response.success) {
        setSuccess(response.message + ' Verifying token...');
        setRequestToken('');
        setLoginUrl(null);
        // Refresh status and verify token
        await fetchStatus();
        setSuccess(prev => prev?.replace(' Verifying token...', '') + ' Token verified!');
      } else {
        setError(response.message);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to submit token');
    } finally {
      setLoading(false);
    }
  };

  const openLoginUrl = () => {
    if (loginUrl?.login_url) {
      window.open(loginUrl.login_url, '_blank');
    }
  };

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white shadow-sm border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold text-gray-900">Broker Authentication</h1>
              <p className="text-sm text-gray-500 mt-1">
                Authenticate with your broker to enable trading
              </p>
            </div>
            <Link href="/" className="btn-secondary">
              ← Back to Dashboard
            </Link>
          </div>
        </div>
      </header>

      <main className="max-w-3xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Status Card */}
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6 mb-6">
          <h2 className="text-lg font-medium text-gray-900 mb-4">Authentication Status</h2>
          
          {status ? (
            <div className="space-y-3">
              <div className="flex items-center space-x-3">
                <div className={`w-3 h-3 rounded-full ${status.authenticated ? 'bg-green-500' : 'bg-red-500'}`}></div>
                <div>
                  <p className={`font-medium ${status.authenticated ? 'text-green-700' : 'text-red-700'}`}>
                    {status.authenticated ? 'Authenticated' : 'Not Authenticated'}
                  </p>
                  <p className="text-sm text-gray-500">{status.message}</p>
                </div>
              </div>
              
              {/* Token Verification Result */}
              {verifyResult && (
                <div className={`mt-3 p-3 rounded-lg ${verifyResult.valid ? 'bg-green-50 border border-green-200' : 'bg-red-50 border border-red-200'}`}>
                  <p className={`text-sm font-medium ${verifyResult.valid ? 'text-green-800' : 'text-red-800'}`}>
                    {verifyResult.valid ? '✓ Token Verified' : '✗ Token Invalid'}
                  </p>
                  <p className={`text-xs mt-1 ${verifyResult.valid ? 'text-green-700' : 'text-red-700'}`}>
                    {verifyResult.message}
                  </p>
                  {verifyResult.profile && verifyResult.valid && (
                    <div className="mt-2 text-xs text-green-700">
                      <p>User: {(verifyResult.profile as { user_name?: string }).user_name || 'N/A'}</p>
                      <p>Email: {(verifyResult.profile as { email?: string }).email || 'N/A'}</p>
                    </div>
                  )}
                </div>
              )}
            </div>
          ) : (
            <div className="animate-pulse flex space-x-4">
              <div className="h-3 w-3 bg-gray-200 rounded-full"></div>
              <div className="flex-1 space-y-2">
                <div className="h-4 bg-gray-200 rounded w-1/4"></div>
                <div className="h-3 bg-gray-200 rounded w-1/2"></div>
              </div>
            </div>
          )}
        </div>

        {/* Alerts */}
        {error && (
          <div className="bg-red-50 border border-red-200 rounded-lg p-4 mb-6">
            <p className="text-red-800">{error}</p>
            <button 
              onClick={() => setError(null)}
              className="text-red-600 text-sm mt-2 hover:underline"
            >
              Dismiss
            </button>
          </div>
        )}

        {success && (
          <div className="bg-green-50 border border-green-200 rounded-lg p-4 mb-6">
            <p className="text-green-800">{success}</p>
            <button 
              onClick={() => setSuccess(null)}
              className="text-green-600 text-sm mt-2 hover:underline"
            >
              Dismiss
            </button>
          </div>
        )}

        {/* Authentication Flow */}
        {!status?.authenticated && (
          <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
            <h2 className="text-lg font-medium text-gray-900 mb-4">Authenticate with Broker</h2>
            
            {!loginUrl ? (
              <div className="text-center py-8">
                <p className="text-gray-600 mb-6">
                  Click the button below to generate a login URL for your broker.
                  You will be redirected to the broker&apos;s login page.
                </p>
                <button
                  onClick={handleGetLoginUrl}
                  disabled={loading}
                  className="btn-primary"
                >
                  {loading ? 'Generating...' : 'Get Login URL'}
                </button>
              </div>
            ) : (
              <div className="space-y-6">
                {loginUrl.success ? (
                  <>
                    <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
                      <p className="text-blue-800 font-medium mb-2">Step 1: Login with your broker</p>
                      <p className="text-blue-700 text-sm mb-4">
                        Click the button below to open the broker login page. After logging in, 
                        you will be redirected to a page with a request token in the URL.
                      </p>
                      <button
                        onClick={openLoginUrl}
                        className="btn-primary bg-blue-600 hover:bg-blue-700"
                      >
                        Open Broker Login Page
                      </button>
                    </div>

                    <div className="border-t border-gray-200 pt-6">
                      <p className="text-gray-800 font-medium mb-2">Step 2: Enter Request Token</p>
                      <p className="text-gray-600 text-sm mb-4">
                        Copy the request token from the URL after login and paste it below.
                        The token looks like: <code className="bg-gray-100 px-1 py-0.5 rounded">?request_token=xxx</code>
                      </p>
                      <div className="flex space-x-3">
                        <input
                          type="text"
                          value={requestToken}
                          onChange={(e) => setRequestToken(e.target.value)}
                          placeholder="Paste request token here"
                          className="flex-1 px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent"
                        />
                        <button
                          onClick={handleSubmitToken}
                          disabled={loading || !requestToken.trim()}
                          className="btn-primary"
                        >
                          {loading ? 'Submitting...' : 'Submit Token'}
                        </button>
                      </div>
                    </div>

                    <div className="border-t border-gray-200 pt-4">
                      <button
                        onClick={() => setLoginUrl(null)}
                        className="text-gray-600 hover:text-gray-800 text-sm"
                      >
                        ← Start Over
                      </button>
                    </div>
                  </>
                ) : (
                  <div className="text-center py-4">
                    <p className="text-red-600 mb-4">{loginUrl.message}</p>
                    <button
                      onClick={() => setLoginUrl(null)}
                      className="btn-secondary"
                    >
                      Try Again
                    </button>
                  </div>
                )}
              </div>
            )}
          </div>
        )}

        {/* Already Authenticated */}
        {status?.authenticated && (
          <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6 text-center">
            <div className="w-16 h-16 bg-green-100 rounded-full flex items-center justify-center mx-auto mb-4">
              <svg className="w-8 h-8 text-green-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
              </svg>
            </div>
            <h3 className="text-lg font-medium text-gray-900 mb-2">Already Authenticated</h3>
            <p className="text-gray-600 mb-6">
              You are currently authenticated with {status.broker}. 
              You can start trading from the dashboard.
            </p>
            <div className="flex justify-center space-x-4">
              <Link href="/" className="btn-primary">
                Go to Dashboard
              </Link>
              <button
                onClick={async () => {
                  setLoading(true);
                  await fetchStatus();
                  setLoading(false);
                }}
                disabled={loading}
                className="btn-secondary"
              >
                {loading ? 'Testing...' : 'Test Connection'}
              </button>
              <button
                onClick={handleGetLoginUrl}
                className="btn-secondary"
              >
                Re-authenticate
              </button>
            </div>
          </div>
        )}

        {/* Instructions */}
        <div className="mt-8 bg-gray-50 rounded-xl border border-gray-200 p-6">
          <h3 className="text-sm font-medium text-gray-900 mb-3">How it works:</h3>
          <ol className="text-sm text-gray-600 space-y-2 list-decimal list-inside">
            <li>Click &quot;Get Login URL&quot; to generate a broker login link</li>
            <li>Open the login URL and enter your broker credentials</li>
            <li>After successful login, copy the request token from the browser URL</li>
            <li>Paste the request token here and click &quot;Submit Token&quot;</li>
            <li>The access token will be saved automatically</li>
          </ol>
        </div>
      </main>
    </div>
  );
}
