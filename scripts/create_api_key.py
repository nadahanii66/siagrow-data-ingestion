import hashlib
import secrets
import sys

customer_id = sys.argv[1] if len(sys.argv) > 1 else "customer"
key = f"sk_{customer_id}_{secrets.token_urlsafe(24)}"

print("Give this key to the customer. It is shown only once:")
print(f"  {key}")
print()
print("Put this line under the customer in config/customers.yaml:")
print(f"    api_key_sha256: {hashlib.sha256(key.encode()).hexdigest()}")