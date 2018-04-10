import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
CRYPTO_DIR = os.path.join(DATA_DIR, "crypto")
IMG_DIR = os.path.join(PROJECT_ROOT, "img")

dirs = [PROJECT_ROOT, DATA_DIR, CRYPTO_DIR, IMG_DIR]

for dir in dirs:
    if not os.path.exists(dir):
        os.makedirs(dir)