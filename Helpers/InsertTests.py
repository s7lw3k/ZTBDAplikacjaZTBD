import json
import random
import sqlite3
import time

import chromadb
import matplotlib.pyplot as plt
import pandas as pd

from Consts.consts import PANDAS_DATA_PATH, SQLITE_DB_PATH, CHROMA_COLLECTION_NAME, CHROMA_PORT, CHROMA_HOST


def getChromaCollection(client: chromadb.HttpClient):
    return client.get_or_create_collection(name=CHROMA_COLLECTION_NAME)


def removeAndGetChromaCollection(client: chromadb.HttpClient):
    client.delete_collection(name=CHROMA_COLLECTION_NAME)
    return client.get_or_create_collection(name=CHROMA_COLLECTION_NAME)


def Chroma_insert_data_from_dataframe(df, number_of_rows, collection: chromadb.Collection) -> float:
    drug_name = df.iloc[:, 1].tolist()[:number_of_rows]
    condition = df.iloc[:, 2].tolist()[:number_of_rows]
    docs = df.iloc[:, 3].tolist()[:number_of_rows]
    rating = df.iloc[:, 4].tolist()[:number_of_rows]
    metadata = [{'source': PANDAS_DATA_PATH,
                 'drugName': drug_name[i],
                 'condition': condition[i],
                 'rating': rating[i]} for i in range(len(rating))]
    rand = random.random()
    ids = [str(x * rand) for x in df.iloc[:, 0].to_list()[:number_of_rows]]

    start_time = time.perf_counter()
    collection.add(
        documents=docs,
        metadatas=metadata,
        ids=ids
    )
    end_time = time.perf_counter()
    return end_time - start_time


def SQLite_insert_data_from_dataframe(df, num_rows):
    conn = sqlite3.connect(SQLITE_DB_PATH)
    cursor = conn.cursor()
    for index, row in df[['drugName', 'condition']].head(num_rows).iterrows():
        cursor.execute("INSERT INTO drugNames (drugName, condition) VALUES (?, ?)", (row['drugName'], row['condition']))

    # Insert data into reviews table
    for index, row in df[['drugName', 'review', 'usefulCount']].head(num_rows).iterrows():
        drug_id = cursor.execute("SELECT id FROM drugNames WHERE drugName=?", (row['drugName'],)).fetchone()[0]
        cursor.execute("INSERT INTO reviews (drug_id, review, usefulCount) VALUES (?, ?, ?)",
                       (drug_id, row['review'], row['usefulCount']))

    # Insert data into metadata table
    for index, row in df[['drugName', 'rating', 'date']].head(num_rows).iterrows():
        drug_id = cursor.execute("SELECT id FROM drugNames WHERE drugName=?", (row['drugName'],)).fetchone()[0]
        cursor.execute("INSERT INTO metadata (drug_id, rating, date) VALUES (?, ?, ?)",
                       (drug_id, row['rating'], row['date']))
    conn.commit()
    conn.close()


def delete_all_rows() -> None:
    conn = sqlite3.connect(SQLITE_DB_PATH)
    cursor = conn.cursor()
    cursor.execute('DELETE FROM drugNames')
    cursor.execute('DELETE FROM reviews')
    cursor.execute('DELETE FROM metadata')
    conn.commit()
    conn.close()


def create_tables() -> None:
    conn = sqlite3.connect(SQLITE_DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS drugNames
                    (id INTEGER PRIMARY KEY, drugName TEXT, condition TEXT)''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS reviews
                    (id INTEGER PRIMARY KEY, drug_id INTEGER,
                    review TEXT, usefulCount INTEGER,
                    FOREIGN KEY (drug_id) REFERENCES drugNames(id))''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS metadata
                    (id INTEGER PRIMARY KEY, drug_id INTEGER,
                    rating REAL, date TEXT,
                    FOREIGN KEY (drug_id) REFERENCES drugNames(id))''')
    conn.commit()
    conn.close()


def SQLite_test_insert(options: list, df: pd.DataFrame) -> list[dict[str, str]]:
    delete_all_rows()
    values = []
    for option in options:
        start_time = time.perf_counter()
        SQLite_insert_data_from_dataframe(df, option)
        end_time = time.perf_counter()
        values.append({'Element count': option, 'SQLite Time': end_time - start_time})
    return values


def Chroma_test_insert(options: list, df: pd.DataFrame) -> list[dict[str, str]]:
    choromaClient = chromadb.HttpClient(host=CHROMA_HOST, port=CHROMA_PORT)
    values = []

    for option in options:
        chromaCollection = getChromaCollection(choromaClient)
        ch_time = Chroma_insert_data_from_dataframe(df, option, chromaCollection)
        values.append({'Element count': option, 'Chroma Time': ch_time})
    return values


def describe_test() -> None:
    print("--------------------------------------------------")
    print("-------------------INSERT TEST--------------------")
    print("(może zająć kilkadziesiąt minut)")
    print("-Ten test usuwa wszystkie wcześniej istniejące dane")
    print("-Wstawia dane w ilościach [1, 10, 50, 100, 1000, 10_000, 41666]")
    print("-Wstawianie ilości 41666 jest powtarzane 5 razy")
    print("-41666 to maksmalny rozmiar paczki danych jaką można wstawić do chroma")
    print("-Wyniki są zbierane i zapisywane do pliku insert_results.json")
    print("-Na podstawie wyników tworzone są wykresy")
    print("--------------------------------------------------")


def end_message() -> None:
    print("--------------------------------------------------")
    print("-----------------TEST ZAKONCZONY------------------")


def save_results_to_file(fielName: str, results: list) -> None:
    with open(fielName, 'w') as file:
        json.dump(results, file, indent=2)


def make_chart(title: str, x_name: str, y_name: str, x: list, ys: list, xscale: str = 'linear',
               yscale: str = 'linear') -> None:
    plt.plot(x, ys[0], label='SQLite', marker='o')
    plt.plot(x, ys[1], label='Chroma', marker='o')
    plt.title(title)
    plt.xlabel(x_name)
    plt.ylabel(y_name)
    plt.grid(True)
    plt.xscale(xscale)
    plt.yscale(yscale)
    plt.legend()
    plt.savefig(f'./Charts/{title}.png')
    plt.close()


def make_chart_simple(title: str, x_name: str, y_name: str, x: list, y: list, xscale: str = 'linear',
                      yscale: str = 'linear') -> None:
    plt.plot(x, y, label='Chroma', marker='o')
    plt.title(title)
    plt.xlabel(x_name)
    plt.ylabel(y_name)
    plt.grid(True)
    plt.xscale(xscale)
    plt.yscale(yscale)
    plt.legend()
    plt.savefig(f'./Charts/{title}.png')
    plt.close()


def make_charts(data: list):
    quantity_ys = [[], []]
    quantity_x = []
    continuity_ys = [[], []]
    continuity_x = []
    for s, c in zip(data[0][:-4][0], data[1][:-4][0]):
        quantity_ys[0].append(s['SQLite Time'])
        quantity_ys[1].append(c['Chroma Time'])
        quantity_x.append(s['Element count'])
    make_chart("Czas w zależności od ilości elementów",
               "Ilość Elementów",
               "Czas[s]",
               quantity_x,
               quantity_ys,
               'log',
               'log')
    idx = 2
    continuity_x.append(1)
    continuity_ys[0].append(data[0][-3][0]['SQLite Time'])
    continuity_ys[1].append(data[1][-3][0]['Chroma Time'])
    for s, c in zip(data[0][-4:], data[1][-4:]):
        continuity_ys[0].append(s[0]['SQLite Time'])
        continuity_ys[1].append(c[0]['Chroma Time'])
        continuity_x.append(idx)
        idx += 1
    make_chart("Stabilonść czasów dla wstawianych danych",
               "Numer próby",
               "Czas[s]",
               continuity_x,
               continuity_ys)


def warning() -> bool:
    print("--------------------------------------------------")
    print("----UWAGA TA FUNCKJA JEST BARDZO CZASOCHŁONNA----")
    print("Jeżeli chcesz wrócić kliknij (d + Enter)")
    print("W innym wypadku kliknij (Enter)")
    print("--------------------------------------------------")
    choice = input("")
    return choice == "d" or choice == "D"


def perform_insert_test() -> None:
    if warning():
        return None
    df = pd.read_csv(PANDAS_DATA_PATH, sep=',')
    describe_test()
    print("Rozpoczęcie testu nr: 1")
    print(f"Wstawianie SQLite")
    sql_res = [SQLite_test_insert([1, 10, 50, 100, 1000, 10_000, 41_666], df)]
    print(f"Wstawianie Chroma")
    chroma_res = [Chroma_test_insert([1, 10, 50, 100, 1000, 10_000, 41_666], df)]
    for i in range(4):
        print(f"Rozpoczęcie testu nr: {i + 2}")
        print(f"\rWstawianie SQLite")
        sql_res.append(SQLite_test_insert([41_666], df))
        print(f"\rWstawianie Chroma")
        chroma_res.append(Chroma_test_insert([41_666], df))
    print("\rZapisywanie wyników do pliku")
    save_results_to_file('insert_results.json', [sql_res, chroma_res])
    print("\rTworzenie wykresów")
    make_charts([sql_res, chroma_res])
    end_message()
