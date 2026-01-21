import os
from typing import Any, Dict, Optional, Tuple

import requests


class PocketBaseClient:
    """
    Very small PocketBase client using HTTP APIs:
    - Auth against an auth collection (defaults to users)
    - Upsert into ing_items by (source, source_id)
    """

    def __init__(
        self,
        base_url: str,
        auth_collection: str,
        email: str,
        password: str,
        timeout_s: int = 20,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.auth_collection = auth_collection
        self.email = email
        self.password = password
        self.timeout_s = timeout_s
        self._token: Optional[str] = None

    def _headers(self) -> Dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        return headers

    def login(self) -> None:
        url = f"{self.base_url}/api/collections/{self.auth_collection}/auth-with-password"
        resp = requests.post(
            url,
            headers={"Content-Type": "application/json"},
            json={"identity": self.email, "password": self.password},
            timeout=self.timeout_s,
        )
        resp.raise_for_status()
        data = resp.json()
        token = data.get("token")
        if not token:
            raise RuntimeError("PocketBase auth succeeded but no token returned.")
        self._token = token

    def _ensure_auth(self) -> None:
        if not self._token:
            self.login()

    def get_one_by_filter(self, collection: str, filter_expr: str) -> Optional[Dict[str, Any]]:
        self._ensure_auth()
        url = f"{self.base_url}/api/collections/{collection}/records"
        resp = requests.get(
            url,
            headers=self._headers(),
            params={"filter": filter_expr, "perPage": 1, "page": 1},
            timeout=self.timeout_s,
        )
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items") or []
        return items[0] if items else None

    def create(self, collection: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        self._ensure_auth()
        url = f"{self.base_url}/api/collections/{collection}/records"
        resp = requests.post(url, headers=self._headers(), json=payload, timeout=self.timeout_s)
        resp.raise_for_status()
        return resp.json()

    def update(self, collection: str, record_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        self._ensure_auth()
        url = f"{self.base_url}/api/collections/{collection}/records/{record_id}"
        resp = requests.patch(url, headers=self._headers(), json=payload, timeout=self.timeout_s)
        resp.raise_for_status()
        return resp.json()

    def upsert_ing_item(self, item: Dict[str, Any]) -> Tuple[str, str]:
        """
        Upsert by (source, source_id).
        Returns (action, record_id) where action in {"created","updated","skipped"}.
        """
        source = item["source"]
        source_id = str(item["source_id"]).replace('"', '\\"')  # naive escape for PB filter

        # If you have a unique index, this is still safest for now (simple).
        existing = self.get_one_by_filter(
            "ing_items",
            f'source="{source}" && source_id="{source_id}"',
        )

        # Skip update if content_hash unchanged
        if existing and existing.get("content_hash") and existing.get("content_hash") == item.get("content_hash"):
            return ("skipped", existing["id"])

        if not existing:
            rec = self.create("ing_items", item)
            return ("created", rec["id"])

        rec = self.update("ing_items", existing["id"], item)
        return ("updated", rec["id"])


def pb_client_from_env() -> PocketBaseClient:
    pb_url = os.getenv("PB_URL", "http://127.0.0.1:8090")
    pb_email = os.getenv("PB_WORKER_EMAIL")
    pb_password = os.getenv("PB_WORKER_PASSWORD")
    auth_collection = os.getenv("PB_AUTH_COLLECTION", "users")

    if not pb_email or not pb_password:
        raise RuntimeError("Missing PB_WORKER_EMAIL / PB_WORKER_PASSWORD env vars.")

    client = PocketBaseClient(pb_url, auth_collection, pb_email, pb_password)
    client.login()
    return client
