import os
from ddm_sdk.client import DdmClient

def main():
    client = DdmClient.from_env()
    username = os.getenv("DDM_USERNAME")
    password = os.getenv("DDM_PASSWORD")
    if not username or not password:
        raise SystemExit("Set DDM_USERNAME and DDM_PASSWORD in .env")

    resp = client.login(username, password)
    print("Logged in, token starts with:", resp.access_token[:20])
    # now call a protected endpoint:
    me = client.user.get_profile(username)  # whatever your UserAPI method is
    print(me)

if __name__ == "__main__":
    main()
