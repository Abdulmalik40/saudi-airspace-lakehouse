"""Throwaway test for TokenManager. Do not commit."""
from ingestion.token_manager import TokenManager

tm = TokenManager()

token1 = tm.get_token()
print(f"Got token, length: {len(token1)}")
print(f"First 30 chars: {token1[:30]}...")
print(f"Expires at: {tm.expires_at}")

token2 = tm.get_token()
print(f"Second call returned cached token: {token1 == token2}")