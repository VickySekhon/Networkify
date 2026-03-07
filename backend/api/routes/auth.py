import base64
import json
import httpx
from fastapi import APIRouter, Request, Depends, HTTPException
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.security import OAuth2PasswordBearer
from authlib.integrations.starlette_client import OAuth
from jose import jwt, JWTError
from datetime import datetime, timedelta
from config import settings

router = APIRouter(prefix="/auth", tags=["auth"])

oauth = OAuth()

oauth.register(
    name="auth0",
    client_id=settings.auth0_client_id,
    client_secret=settings.auth0_client_secret,
    server_metadata_url=f"https://{settings.auth0_domain}/.well-known/openid-configuration",
    client_kwargs={"scope": "openid email profile"},
)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


def create_access_token(user: dict):
    payload = {
        "sub": user["email"],
        "name": user.get("name", ""),
        "picture": user.get("picture", ""),
        "exp": datetime.utcnow() + timedelta(hours=2),
    }
    return jwt.encode(payload, settings.app_secret_key, algorithm="HS256")


def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, settings.app_secret_key, algorithms=["HS256"])
        return payload
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")


@router.get("/login")
async def login(request: Request):
    redirect_uri = request.url_for("auth_callback")
    return await oauth.auth0.authorize_redirect(request, redirect_uri)


@router.post("/login/password")
async def login_password(request: Request):
    try:
        data = await request.json()
        email = data.get("email")
        password = data.get("password")

        if not email or not password:
            raise HTTPException(status_code=400, detail="Email and password required")

        # Call Auth0's oauth/token endpoint using the Resource Owner Password Grant
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"https://{settings.auth0_domain}/oauth/token",
                data={
                    "grant_type": "http://auth0.com/oauth/grant-type/password-realm",
                    "username": email,
                    "password": password,
                    "audience": f"https://{settings.auth0_domain}/userinfo",
                    "client_id": settings.auth0_client_id,
                    "client_secret": settings.auth0_client_secret,
                    "realm": "Username-Password-Authentication",
                    "scope": "openid profile email"
                }
            )
            
            if resp.status_code != 200:
                json_data = resp.json()
                print(f"Auth0 Login Error: {json_data}")
                error_detail = json_data.get("error_description") or json_data.get("description") or "Invalid credentials or login failed"
                raise HTTPException(status_code=resp.status_code, detail=error_detail)
            
            token_data = resp.json()
            # In a real app, we'd exchange the Auth0 ID token for our app's JWT
            # For simplicity, we create our own app token based on the email
            user = {
                "email": email,
                "name": email.split("@")[0], # Fallback name
                "picture": "" # Fallback picture
            }
            
            app_token = create_access_token(user)
            return {"token": app_token, "user": user}
            
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/signup")
async def signup(request: Request):
    try:
        data = await request.json()
        email = data.get("email")
        password = data.get("password")
        name = data.get("name")

        if not email or not password or not name:
            raise HTTPException(status_code=400, detail="Email, password, and name are required")

        # Call Auth0's /dbconnections/signup endpoint
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"https://{settings.auth0_domain}/dbconnections/signup",
                json={
                    "client_id": settings.auth0_client_id,
                    "email": email,
                    "password": password,
                    "connection": "Username-Password-Authentication",
                    "user_metadata": {"full_name": name}
                }
            )
            
            if resp.status_code != 200:
                json_data = resp.json()
                print(f"Auth0 Signup Error: {json_data}")
                error_detail = json_data.get("description") or json_data.get("message") or json_data.get("error") or "Signup failed"
                raise HTTPException(status_code=resp.status_code, detail=error_detail)
            
            # After successful signup, we can technically log them in or ask them to sign in
            # For a better UX, let's create a token immediately
            user = {
                "email": email,
                "name": name,
                "picture": ""
            }
            app_token = create_access_token(user)
            return {"token": app_token, "user": user, "message": "User created successfully"}
            
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/callback", name="auth_callback")
async def auth_callback(request: Request):
    token = await oauth.auth0.authorize_access_token(request)
    user_info = token.get("userinfo") or {}

    user = {
        "email": user_info.get("email", ""),
        "name": user_info.get("name", ""),
        "picture": user_info.get("picture", ""),
    }

    app_token = create_access_token(user)

    # Base64-encode user info for safe URL transport
    user_b64 = base64.urlsafe_b64encode(json.dumps(user).encode()).decode()
    redirect_url = f"{settings.frontend_url}/auth/callback?token={app_token}&user={user_b64}"
    return RedirectResponse(url=redirect_url)


@router.get("/me")
def me(current_user: dict = Depends(get_current_user)):
    return current_user


@router.post("/logout")
def logout():
    return JSONResponse({"message": "Logged out successfully"})