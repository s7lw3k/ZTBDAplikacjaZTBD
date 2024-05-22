import sqlite3

import chromadb

from Consts.consts import CHROMA_HOST, CHROMA_PORT, SQLITE_DB_PATH


def check_chroma_connection():
    try:
        client = chromadb.HttpClient(host=CHROMA_HOST, port=CHROMA_PORT)
        if client.heartbeat():
            print("Connected to ChromaDB.")
    except Exception as e:
        print("Error in ChromaDB:", e)


def check_sqlite_connection():
    try:
        connection = sqlite3.connect(SQLITE_DB_PATH)
        if connection:
            print("Connected to SQLite.")
    except Exception as e:
        print("Error in SQLite:", e)
