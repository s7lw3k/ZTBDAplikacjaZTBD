import math
import sqlite3
import threading
import time

import chromadb
import pandas as pd

from Consts.consts import PANDAS_DATA_PATH, CHROMA_HOST, CHROMA_PORT, MAX_CHROMA_BUNCH_SIZE, SQLITE_DB_PATH
from Helpers.DeleteTests import getChromaCollection, animate, Done
from Helpers.InsertTests import Chroma_insert_data_from_dataframe, SQLite_test_insert, \
    save_results_to_file, make_chart, end_message

d = Done(False)


def updateTestSQLite(num_rows: int):
    conn = sqlite3.connect(SQLITE_DB_PATH)
    cursor = conn.cursor()

    start_time = time.perf_counter()
    cursor.execute(
        'UPDATE drugNames SET condition = "Updated condition" WHERE id IN (SELECT id FROM drugNames LIMIT ?)',
        (num_rows,))
    cursor.execute('UPDATE reviews SET review = "Updated review" WHERE id IN (SELECT id FROM reviews LIMIT ?)',
                   (num_rows,))
    cursor.execute('UPDATE metadata SET rating = 5.0 WHERE id IN (SELECT id FROM metadata LIMIT ?)', (num_rows,))
    end_time = time.perf_counter()

    conn.commit()
    conn.close()
    return {'Element count': num_rows, 'SQLite Time': end_time - start_time}


def updateTestChroma(collection: chromadb.Collection) -> dict[str, int | float]:
    results = collection.query(query_texts='',
                               n_results=30_000)
    newMetadata = results['metadatas']
    for metadata in newMetadata[0]:
        metadata['drugName'] = f"new {metadata['drugName']}"
        metadata['rating'] = metadata['rating'] % 8
    start_time = time.perf_counter()
    collection.upsert(
        ids=results['ids'][0],
        metadatas=newMetadata[0],
        embeddings=results['embeddings'],
        documents=results['documents'][0],
        uris=results['uris'],
    )
    end_time = time.perf_counter()
    return {'Element count': collection.count(), 'Chroma Time': end_time - start_time}


def make_charts(data: list):
    continuity_ys = [[], []]
    continuity_x = []
    idx = 1
    for s, c in zip(data[0], data[1]):
        continuity_ys[0].append(s['SQLite Time'])
        continuity_ys[1].append(c['Chroma Time'])
        continuity_x.append(idx)
        idx += 1
    make_chart(f"Czasy dla aktualizacji danych ({MAX_CHROMA_BUNCH_SIZE} elementów)",
               "Numer próby",
               "Czas[s]",
               continuity_x,
               continuity_ys,
               "linear",
               "log")


def describe_test() -> None:
    print("--------------------------------------------------")
    print("-------------------UPDATE TEST--------------------")
    print("(może zająć kilka minut)")
    print("-Ten test upewnia się, że tabele/kolekcje mają odpowiednij rozmiar")
    print("-Aktualizuje 41666 elementów z obu baz i mierzy czas tej operacji")
    print("-Aktualizacja jest powtarzane 5 razy")
    print("-41666 to maksmalny rozmiar paczki danych jaką można wstawić do chroma")
    print("-Wyniki są zbierane i zapisywane do pliku update_results.json")
    print("-Na podstawie wyników tworzone są wykresy")
    print("--------------------------------------------------")


def perform_update_test():
    describe_test()
    df = pd.read_csv(PANDAS_DATA_PATH, sep=',')
    choromaClient = chromadb.HttpClient(host=CHROMA_HOST, port=CHROMA_PORT)
    collection: chromadb.Collection = getChromaCollection(choromaClient)
    if collection.count() < MAX_CHROMA_BUNCH_SIZE:
        print("Uzupełnianie kolekcji")
        t = threading.Thread(target=animate)
        t.start()
        Chroma_insert_data_from_dataframe(df, MAX_CHROMA_BUNCH_SIZE - collection.count(), collection)
        SQLite_test_insert([MAX_CHROMA_BUNCH_SIZE], df)
        d.toggle()
    chroma_res = []
    sql_res = []
    for i in range(5):
        print(f"Rozpoczęcie testu nr: {i + 1}")
        d.toggle()
        t = threading.Thread(target=animate)
        t.start()
        print(f"\rAktualizacja Chroma")
        chroma_res.append(updateTestChroma(collection))
        print(f"\rAktualizacja SQLite")
        sql_res.append(updateTestSQLite(MAX_CHROMA_BUNCH_SIZE))
        d.toggle()
    print("\rZapisywanie wyników do pliku")
    save_results_to_file('update_results.json', [sql_res, chroma_res])
    print("\rTworzenie wykresów")
    make_charts([sql_res, chroma_res])
    time.sleep(0.5)
    end_message()
