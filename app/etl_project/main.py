import os
from dotenv import load_dotenv

if __name__ == "__main__":
    load_dotenv()
    USERNAME = os.environ.get("USERNAME", "World")
    print(f"Hello {USERNAME}")
