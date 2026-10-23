from __future__ import annotations

import secrets
import uuid
from pathlib import Path
from typing import Any

from authlib.integrations.starlette_client import OAuth
from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from app.config import settings
from app.process import customer_adjustment
from app.temporal_client import connect_temporal


app = FastAPI(title="NexusFlow")
app.add_middleware(
    SessionMiddleware,
    secret_key=settings.session_secret,
    https_only=settings.public_base_url.startswith("https://"),
    same_site="lax",
)
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
oauth = OAuth()
if settings.entra_web_client_id:
    oauth.register(
        name="entra",
        client_id=settings.entra_web_client_id,
        client_secret=settings.entra_web_client_secret,
        server_metadata_url=f"{settings.entra_issuer}/.well-known/openid-configuration",
        client_kwargs={"scope": "openid profile email"},
    )


def _user(request: Request, role: str | None = None) -> dict[str, Any]:
    user = request.session.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="Sign in required")
    if role and role not in user.get("roles", []):
        raise HTTPException(status_code=403, detail="Insufficient role")
    return user


def _csrf(request: Request, supplied: str) -> None:
    if not secrets.compare_digest(request.session.get("csrf", ""), supplied):
        raise HTTPException(status_code=403, detail="Invalid CSRF token")


async def _executions(requested_by: str | None = None, waiting_only: bool = False) -> list[dict[str, Any]]:
    client = await connect_temporal()
    results: list[dict[str, Any]] = []
    async for execution in client.list_workflows('WorkflowType="LightweightProcess"'):
        try:
            status = await client.get_workflow_handle(execution.id, run_id=execution.run_id).query("status")
        except Exception:
            continue
        if requested_by and status.get("requested_by") != requested_by:
            continue
        if waiting_only and status.get("state") != "WAITING_FOR_APPROVAL":
            continue
        status["workflow_id"] = execution.id
        results.append(status)
        if len(results) >= 100:
            break
    return results


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/login")
async def login(request: Request):
    if not settings.entra_web_client_id:
        raise HTTPException(status_code=503, detail="Entra authentication is not configured")
    return await oauth.entra.authorize_redirect(request, f"{settings.public_base_url}/auth/callback")


@app.get("/auth/callback")
async def callback(request: Request):
    token = await oauth.entra.authorize_access_token(request)
    claims = token["userinfo"]
    roles = claims.get("roles", [])
    if not set(roles).intersection({"NexusFlow.Requester", "NexusFlow.Manager"}):
        raise HTTPException(status_code=403, detail="No NexusFlow role assigned")
    request.session["user"] = {
        "oid": claims.get("oid"),
        "username": claims.get("preferred_username") or claims.get("email") or claims.get("oid"),
        "name": claims.get("name", "NexusFlow user"),
        "roles": roles,
    }
    request.session["csrf"] = secrets.token_urlsafe(32)
    return RedirectResponse("/manager" if "NexusFlow.Manager" in roles else "/", status_code=303)


@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(
        f"{settings.entra_issuer}/oauth2/v2.0/logout?post_logout_redirect_uri={settings.public_base_url}",
        status_code=303,
    )


@app.get("/")
async def home(request: Request):
    try:
        user = _user(request, "NexusFlow.Requester")
    except HTTPException as exc:
        if exc.status_code == 401:
            return RedirectResponse("/login", status_code=303)
        raise
    return templates.TemplateResponse(
        request,
        "home.html",
        {"user": user, "csrf": request.session["csrf"], "executions": await _executions(user["username"])},
    )


@app.post("/requests")
async def submit_request(
    request: Request,
    customer_id: str = Form(...),
    amount: float = Form(...),
    reason: str = Form(...),
    simulate_transient_failure: bool = Form(False),
    csrf: str = Form(...),
):
    user = _user(request, "NexusFlow.Requester")
    _csrf(request, csrf)
    if amount <= 0 or not customer_id.strip() or not reason.strip():
        raise HTTPException(status_code=422, detail="Customer, amount, and reason are required")
    workflow_id = f"customer-adjustment-{uuid.uuid4()}"
    client = await connect_temporal()
    await client.start_workflow(
        "LightweightProcess",
        customer_adjustment(
            {
                "customer_id": customer_id.strip(),
                "amount": amount,
                "reason": reason.strip(),
                "simulate_transient_failure": simulate_transient_failure,
            },
            user["username"],
            workflow_id,
        ),
        id=workflow_id,
        task_queue=settings.temporal_task_queue,
    )
    return RedirectResponse("/", status_code=303)


@app.get("/manager")
async def manager(request: Request):
    try:
        user = _user(request, "NexusFlow.Manager")
    except HTTPException as exc:
        if exc.status_code == 401:
            return RedirectResponse("/login", status_code=303)
        raise
    return templates.TemplateResponse(
        request,
        "manager.html",
        {"user": user, "executions": await _executions(waiting_only=True)},
    )


@app.get("/manager/{workflow_id}")
async def manager_detail(request: Request, workflow_id: str):
    user = _user(request, "NexusFlow.Manager")
    status = await (await connect_temporal()).get_workflow_handle(workflow_id).query("status")
    return templates.TemplateResponse(
        request,
        "detail.html",
        {"user": user, "csrf": request.session["csrf"], "workflow_id": workflow_id, "status": status},
    )


@app.post("/manager/{workflow_id}/decision")
async def manager_decision(
    request: Request,
    workflow_id: str,
    approved: bool = Form(...),
    comment: str = Form(""),
    csrf: str = Form(...),
):
    user = _user(request, "NexusFlow.Manager")
    _csrf(request, csrf)
    client = await connect_temporal()
    handle = client.get_workflow_handle(workflow_id)
    status = await handle.query("status")
    if status.get("state") != "WAITING_FOR_APPROVAL":
        raise HTTPException(status_code=409, detail="Request is no longer awaiting approval")
    await handle.signal(
        "approve",
        {"approved": approved, "approver": user["username"], "comment": comment.strip()},
    )
    return RedirectResponse("/manager", status_code=303)
