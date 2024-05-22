import itertools
import math
import sqlite3
import sys
import threading
import time

import chromadb
import pandas as pd

from Consts.consts import PANDAS_DATA_PATH, CHROMA_COLLECTION_NAME, CHROMA_HOST, CHROMA_PORT, MAX_CHROMA_BUNCH_SIZE, \
    SQLITE_DB_PATH
from Helpers.InsertTests import warning, save_results_to_file, make_chart, end_message, \
    Chroma_insert_data_from_dataframe, SQLite_test_insert


class Done():
    done = False

    def __init__(self, done):
        self.done = done

    def isDone(self):
        return self.done

    def toggle(self):
        self.done = not self.done


d = Done(False)


def getChromaCollection(client: chromadb.HttpClient) -> chromadb.Collection:
    return client.get_or_create_collection(name=CHROMA_COLLECTION_NAME)


def deleteTestChroma(collection: chromadb.Collection) -> dict[str, int | float]:
    results = collection.query(query_texts='',
                               n_results=int(math.floor(collection.count()) // 1.1))
    start_time = time.perf_counter()
    collection.delete(
        ids=results['ids'][0]
    )
    end_time = time.perf_counter()
    return {'Element count': collection.count(), 'Chroma Time': end_time - start_time}


def deleteTestSQLite(num_rows: int) -> dict[str, int | float]:
    conn = sqlite3.connect(SQLITE_DB_PATH)
    cursor = conn.cursor()

    start_time = time.perf_counter()
    cursor.execute('DELETE FROM drugNames')
    cursor.execute('DELETE FROM reviews')
    cursor.execute('DELETE FROM metadata')
    end_time = time.perf_counter()

    conn.commit()
    conn.close()
    return {'Element count': num_rows, 'SQLite Time': end_time - start_time}


def describe_test() -> None:
    print("--------------------------------------------------")
    print("-------------------DELETE TEST--------------------")
    print("(może zająć kilkadziesiąt minut)")
    print("-Ten test upewnia się, że tabele/kolekcje mają odpowiednij rozmiar")
    print("-Usuwa 41666 elementów z obu baz i mierzy czas tej operacji")
    print("-Usuwanie jest powtarzane 5 razy")
    print("-41666 to maksmalny rozmiar paczki danych jaką można wstawić do chroma")
    print("-Wyniki są zbierane i zapisywane do pliku delete_results.json")
    print("-Na podstawie wyników tworzone są wykresy")
    print("--------------------------------------------------")


def make_charts(data: list):
    continuity_ys = [[], []]
    continuity_x = []
    idx = 1
    for s, c in zip(data[0], data[1]):
        continuity_ys[0].append(s['SQLite Time'])
        continuity_ys[1].append(c['Chroma Time'])
        continuity_x.append(idx)
        idx += 1
    make_chart(f"Czasy dla usuwania danych ({MAX_CHROMA_BUNCH_SIZE} elementów)",
               "Numer próby",
               "Czas[s]",
               continuity_x,
               continuity_ys,
               "linear",
               "log")


def animate():
    for c in itertools.cycle(['.', '..', '...', '']):
        if d.isDone():
            break
        sys.stdout.write(f'\rWykonywanie funkcji {c}')
        sys.stdout.flush()
        time.sleep(0.4)
    sys.stdout.write('\rSkończone!     \n')


def perform_delete_test() -> None:
    if warning():
        return None
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
        print(f"\rUsuwanie Chroma")
        chroma_res.append(deleteTestChroma(collection))
        print(f"\rUsuwanie SQLite")
        sql_res.append(deleteTestSQLite(MAX_CHROMA_BUNCH_SIZE))
        print(f"\rPonowne wstawienie danych (ten czas się nie liczy)")
        Chroma_insert_data_from_dataframe(df, MAX_CHROMA_BUNCH_SIZE - collection.count(), collection)
        SQLite_test_insert([MAX_CHROMA_BUNCH_SIZE], df)
        d.toggle()
    print("\rZapisywanie wyników do pliku")
    save_results_to_file('delete_results.json', [sql_res, chroma_res])
    print("\rTworzenie wykresów")

    make_charts([sql_res, chroma_res])
    time.sleep(0.5)
    end_message()
