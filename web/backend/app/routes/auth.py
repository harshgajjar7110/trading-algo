"""
Authentication API Routes

Endpoints for broker authentication and token management.
"""
import os
from typing import Dict, Any, Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

router = APIRouter(prefix="/auth", tags=["Authentication"])


class LoginUrlResponse(BaseModel):
    """Response model for login URL generation."""
    success: bool
    login_url: Optional[str] = None
    broker: str
    message: str


class AuthCallbackRequest(BaseModel):
    """Request model for auth callback."""
    request_token: str


class AuthCallbackResponse(BaseModel):
    """Response model for auth callback."""
    success: bool
    message: str
    access_token: Optional[str] = None


class AuthStatusResponse(BaseModel):
    """Response model for auth status."""
    authenticated: bool
    broker: str
    message: str


def _get_env_file_path() -> str:
    """Get the path to the .env file in project root."""
    from pathlib import Path
    # Navigate up from this file to find project root
    current = Path(__file__).resolve()
    for _ in range(5):  # routes -> app -> backend -> web -> project_root
        current = current.parent
        env_file = current / ".env"
        if env_file.exists():
            print(f"[Auth] Found .env file at: {env_file}")
            return str(env_file)
    # Fallback to current working directory
    fallback = Path.cwd() / ".env"
    print(f"[Auth] Using fallback .env at: {fallback}")
    return str(fallback)


def _save_access_token_to_env(access_token: str) -> bool:
    """Save access token to .env file."""
    try:
        env_path = _get_env_file_path()
        print(f"[Auth] Saving access token to: {env_path}")
        
        # Read existing content
        existing_content = ""
        if os.path.exists(env_path):
            with open(env_path, "r") as f:
                existing_content = f.read()
        
        lines = existing_content.splitlines()
        new_lines = []
        access_token_found = False
        
        for line in lines:
            if line.startswith("BROKER_ACCESS_TOKEN="):
                new_lines.append(f"BROKER_ACCESS_TOKEN={access_token}")
                access_token_found = True
                print(f"[Auth] Updated existing BROKER_ACCESS_TOKEN")
            else:
                new_lines.append(line)
        
        if not access_token_found:
            # Add access token after other broker config
            broker_lines = [l for l in lines if l.startswith("BROKER_")]
            if broker_lines:
                # Insert after last BROKER_ line
                last_broker_idx = max(i for i, l in enumerate(lines) if l.startswith("BROKER_"))
                new_lines = lines[:last_broker_idx + 1] + [f"BROKER_ACCESS_TOKEN={access_token}"] + lines[last_broker_idx + 1:]
            else:
                new_lines.append(f"BROKER_ACCESS_TOKEN={access_token}")
            print(f"[Auth] Added new BROKER_ACCESS_TOKEN")
        
        # Write back
        with open(env_path, "w") as f:
            f.write("\n".join(new_lines) + "\n")
        
        # Also update current environment
        os.environ["BROKER_ACCESS_TOKEN"] = access_token
        print(f"[Auth] Updated os.environ['BROKER_ACCESS_TOKEN']")
        
        return True
    except Exception as e:
        print(f"[Auth] Error saving access token: {e}")
        import traceback
        traceback.print_exc()
        return False


@router.get("/login-url", response_model=LoginUrlResponse)
async def get_login_url():
    """
    Generate a login URL for the configured broker.
    
    Returns:
        LoginUrlResponse: Contains the login URL to redirect the user to
    """
    broker_name = (os.getenv("BROKER_NAME") or "zerodha").lower()
    
    try:
        if broker_name == "zerodha":
            return await _get_zerodha_login_url()
        elif broker_name == "fyers":
            return await _get_fyers_login_url()
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported broker: {broker_name}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def _get_zerodha_login_url() -> LoginUrlResponse:
    """Generate Zerodha login URL."""
    try:
        from kiteconnect import KiteConnect
    except ImportError:
        return LoginUrlResponse(
            success=False,
            login_url=None,
            broker="zerodha",
            message="kiteconnect package not installed. Run: pip install kiteconnect"
        )
    
    api_key = os.getenv("BROKER_API_KEY") or os.getenv("KITE_API_KEY") or os.getenv("ZERODHA_API_KEY")
    
    if not api_key:
        return LoginUrlResponse(
            success=False,
            login_url=None,
            broker="zerodha",
            message="BROKER_API_KEY not configured in .env file"
        )
    
    try:
        kite = KiteConnect(api_key=api_key)
        login_url = kite.login_url()
        
        return LoginUrlResponse(
            success=True,
            login_url=login_url,
            broker="zerodha",
            message="Login URL generated successfully. Open this URL in your browser and login."
        )
    except Exception as e:
        return LoginUrlResponse(
            success=False,
            login_url=None,
            broker="zerodha",
            message=f"Failed to generate login URL: {str(e)}"
        )


async def _get_fyers_login_url() -> LoginUrlResponse:
    """Generate Fyers login URL."""
    api_key = os.getenv("BROKER_API_KEY") or os.getenv("FYERS_API_KEY")
    redirect_uri = os.getenv("BROKER_TOTP_REDIRECT_URI") or os.getenv("BROKER_TOTP_REDIDRECT_URI")
    
    if not api_key:
        return LoginUrlResponse(
            success=False,
            login_url=None,
            broker="fyers",
            message="BROKER_API_KEY not configured in .env file"
        )
    
    if not redirect_uri:
        return LoginUrlResponse(
            success=False,
            login_url=None,
            broker="fyers",
            message="BROKER_TOTP_REDIRECT_URI not configured in .env file"
        )
    
    # Fyers uses OAuth2 - construct the authorization URL
    login_url = f"https://api.fyers.in/api/v2/generate-authcode?client_id={api_key}&redirect_uri={redirect_uri}&response_type=code&state=auth"
    
    return LoginUrlResponse(
        success=True,
        login_url=login_url,
        broker="fyers",
        message="Login URL generated successfully. Open this URL in your browser and login."
    )


@router.post("/callback", response_model=AuthCallbackResponse)
async def auth_callback(request: AuthCallbackRequest):
    """
    Handle auth callback from broker.
    
    Args:
        request: Contains the request_token from broker redirect
        
    Returns:
        AuthCallbackResponse: Success/failure status with access token
    """
    broker_name = (os.getenv("BROKER_NAME") or "zerodha").lower()
    
    try:
        if broker_name == "zerodha":
            return await _handle_zerodha_callback(request.request_token)
        elif broker_name == "fyers":
            return await _handle_fyers_callback(request.request_token)
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported broker: {broker_name}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def _handle_zerodha_callback(request_token: str) -> AuthCallbackResponse:
    """Handle Zerodha auth callback."""
    try:
        from kiteconnect import KiteConnect
    except ImportError:
        return AuthCallbackResponse(
            success=False,
            message="kiteconnect package not installed"
        )
    
    api_key = os.getenv("BROKER_API_KEY") or os.getenv("KITE_API_KEY") or os.getenv("ZERODHA_API_KEY")
    api_secret = os.getenv("BROKER_API_SECRET") or os.getenv("KITE_API_SECRET") or os.getenv("ZERODHA_API_SECRET")
    
    if not api_key or not api_secret:
        return AuthCallbackResponse(
            success=False,
            message="BROKER_API_KEY or BROKER_API_SECRET not configured"
        )
    
    try:
        kite = KiteConnect(api_key=api_key)
        session = kite.generate_session(request_token, api_secret)
        access_token = session.get("access_token")
        
        if not access_token:
            return AuthCallbackResponse(
                success=False,
                message="Failed to get access token from broker"
            )
        
        # Save to .env file
        if _save_access_token_to_env(access_token):
            # Re-initialize broker service
            _reinitialize_broker()
            
            return AuthCallbackResponse(
                success=True,
                message="Authentication successful! Access token saved.",
                access_token=access_token[:10] + "..."  # Only return partial for security
            )
        else:
            return AuthCallbackResponse(
                success=False,
                message="Failed to save access token"
            )
            
    except Exception as e:
        return AuthCallbackResponse(
            success=False,
            message=f"Authentication failed: {str(e)}"
        )


async def _handle_fyers_callback(auth_code: str) -> AuthCallbackResponse:
    """Handle Fyers auth callback."""
    import hashlib
    import requests
    
    client_id = os.getenv("BROKER_API_KEY") or os.getenv("FYERS_API_KEY")
    secret_key = os.getenv("BROKER_API_SECRET") or os.getenv("FYERS_API_SECRET")
    
    if not client_id or not secret_key:
        return AuthCallbackResponse(
            success=False,
            message="BROKER_API_KEY or BROKER_API_SECRET not configured"
        )
    
    try:
        # Generate SHA-256 hash of client_id:secret_key
        checksum_input = f"{client_id}:{secret_key}"
        app_id_hash = hashlib.sha256(checksum_input.encode("utf-8")).hexdigest()
        
        # Exchange auth code for access token
        response = requests.post(
            "https://api-t1.fyers.in/api/v3/validate-authcode",
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            json={
                "grant_type": "authorization_code",
                "appIdHash": app_id_hash,
                "code": auth_code
            },
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        
        if data.get("s") != "ok" or not data.get("access_token"):
            return AuthCallbackResponse(
                success=False,
                message=f"Failed to get access token: {data.get('message', 'Unknown error')}"
            )
        
        access_token = data.get("access_token")
        
        # Save to .env file
        if _save_access_token_to_env(access_token):
            # Re-initialize broker service
            _reinitialize_broker()
            
            return AuthCallbackResponse(
                success=True,
                message="Authentication successful! Access token saved.",
                access_token=access_token[:10] + "..."
            )
        else:
            return AuthCallbackResponse(
                success=False,
                message="Failed to save access token"
            )
            
    except Exception as e:
        return AuthCallbackResponse(
            success=False,
            message=f"Authentication failed: {str(e)}"
        )


class TokenVerifyResponse(BaseModel):
    """Response for token verification."""
    valid: bool
    message: str
    profile: Optional[Dict[str, Any]] = None


@router.get("/verify", response_model=TokenVerifyResponse)
async def verify_token():
    """
    Verify that the current access token works by making a test API call.
    
    Returns:
        TokenVerifyResponse: Whether the token is valid and working
    """
    try:
        from app.services.broker_service import broker_service
        broker = broker_service._ensure_broker()
        
        # Try to get funds as a test call
        funds = broker.get_funds()
        
        # Check if we got an error response
        if funds.raw and isinstance(funds.raw, dict) and funds.raw.get("error"):
            return TokenVerifyResponse(
                valid=False,
                message=f"Token validation failed: {funds.raw.get('error')}",
                profile=None
            )
        
        # Also try to get profile
        profile = broker.driver.get_profile() if hasattr(broker, 'driver') else None
        
        return TokenVerifyResponse(
            valid=True,
            message="Token is valid and working",
            profile=profile if isinstance(profile, dict) else None
        )
        
    except Exception as e:
        error_msg = str(e)
        if "api_key" in error_msg.lower() or "access_token" in error_msg.lower():
            return TokenVerifyResponse(
                valid=False,
                message=f"Invalid token: {error_msg}",
                profile=None
            )
        return TokenVerifyResponse(
            valid=False,
            message=f"Token verification error: {error_msg}",
            profile=None
        )


@router.get("/status", response_model=AuthStatusResponse)
async def get_auth_status():
    """
    Get current authentication status.
    
    Returns:
        AuthStatusResponse: Whether the broker is authenticated
    """
    broker_name = (os.getenv("BROKER_NAME") or "zerodha").lower()
    access_token = os.getenv("BROKER_ACCESS_TOKEN") or os.getenv("KITE_ACCESS_TOKEN") or os.getenv("FYERS_ACCESS_TOKEN")
    
    # Check if token exists
    if not access_token:
        return AuthStatusResponse(
            authenticated=False,
            broker=broker_name,
            message=f"No access token found. Please authenticate."
        )
    
    # Token exists, but let's verify it works
    try:
        from app.services.broker_service import broker_service
        broker = broker_service._ensure_broker()
        funds = broker.get_funds()
        
        if funds.raw and isinstance(funds.raw, dict) and funds.raw.get("error"):
            return AuthStatusResponse(
                authenticated=False,
                broker=broker_name,
                message=f"Token invalid: {funds.raw.get('error')}"
            )
        
        return AuthStatusResponse(
            authenticated=True,
            broker=broker_name,
            message=f"Authenticated with {broker_name}"
        )
        
    except Exception as e:
        return AuthStatusResponse(
            authenticated=False,
            broker=broker_name,
            message=f"Token validation failed: {str(e)}"
        )


def _reinitialize_broker():
    """Re-initialize the broker service with new credentials."""
    try:
        from app.services.broker_service import broker_service
        broker_service.reinitialize()
    except Exception as e:
        print(f"Warning: Could not reinitialize broker service: {e}")
