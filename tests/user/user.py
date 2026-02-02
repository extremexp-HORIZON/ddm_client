import os
from ddm_sdk.client import DdmClient

def main():
    client = DdmClient.from_env()

    username = os.getenv("DDM_USERNAME")
    password = os.getenv("DDM_PASSWORD")
    client.login(username, password)

    me = client.user.get_profile(username)
    print("Me:", me.user.username, me.user.email)

    # notifications
    unread = client.user.list_notifications(onlyUnread=True, limit=10)
    print("Unread notifications:", len(unread.data))

if __name__ == "__main__":
    main()
