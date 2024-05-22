import math
import random
import sqlite3
import threading
import time

import chromadb
import pandas as pd

from Consts.consts import PANDAS_DATA_PATH, CHROMA_HOST, CHROMA_PORT, MAX_CHROMA_BUNCH_SIZE, SQLITE_DB_PATH
from Helpers.DeleteTests import animate, Done
from Helpers.InsertTests import getChromaCollection, Chroma_insert_data_from_dataframe, \
    SQLite_test_insert, save_results_to_file, end_message, make_chart, make_chart_simple

d = Done(False)

possibleQueries = ['very bad', 'I love it',
                   'Absolutely horrible, i can\'t understand how they could make something that bad. I feel ill, hate the way they treay anbother human persone. Just unbelivable.',
                   'Litwo! Ojczyzno moja! ty jesteś jak zdrowie. Ile cię trzeba cenić, ten tylko się dowie,Kto cię stracił. Dziś piękność twą w całej ozdobieWidzę i opisuję, bo tęsknię po tobie.Panno Święta, co Jasnej bronisz CzęstochowyI w Ostrej świecisz Bramie! Ty, co gród zamkowyNowogródzki ochraniasz z jego wiernym ludem!Jak mnie dziecko do zdrowia powróciłaś cudem(Gdy od płaczącej matki pod Twoję opiekęOfiarowany, martwą podniosłem powiekę I zaraz mogłem pieszo do Twych świątyń proguIść za wrócone życie podziękować Bogu),Tak nas powrócisz cudem na Ojczyzny łono.Tymczasem przenoś moję duszę utęsknionąDo tych pagórków leśnych, do tych łąk zielonych,Szeroko nad błękitnym Niemnem rozciągnionych;Do tych pól malowanych zbożem rozmaitem,Wyzłacanych pszenicą, posrebrzanych żytem;Gdzie bursztynowy świerzop, gryka jak śnieg biała,Gdzie panieńskim rumieńcem dzięcielina pała,A wszystko przepasane, jakby wstęgą, miedzą Zieloną, na niej z rzadka ciche grusze siedzą.',
                   'What the hell! something amazing', 'can u imagine they making that free', '', 'nice']


def simpleTestChroma(collection: chromadb.Collection) -> dict[str, int | float]:
    start_time = time.perf_counter()
    collection.query(query_texts=random.choices(possibleQueries),
                     n_results=30_000)
    end_time = time.perf_counter()
    return {'Element count': collection.count(), 'Chroma Time': end_time - start_time}


def simpleTestSQLite() -> dict[str, int | float]:
    conn = sqlite3.connect(SQLITE_DB_PATH)
    cursor = conn.cursor()
    start_time = time.perf_counter()
    cursor.execute('''SELECT * FROM reviews''')
    end_time = time.perf_counter()
    rows = cursor.fetchall()

    row_count = len(rows)
    conn.commit()
    conn.close()
    return {'Element count': row_count, 'SQLite Time': end_time - start_time}


def advancedTestSQLite() -> dict[str, int | float]:
    conn = sqlite3.connect(SQLITE_DB_PATH)
    cursor = conn.cursor()
    query = '''
    SELECT 
        dn.id AS drug_id,
        dn.drugName,
        dn.condition,
        r.id AS review_id,
        r.review,
        r.usefulCount,
        m.id AS metadata_id,
        m.rating,
        m.date,
        AVG(m.rating) OVER (PARTITION BY dn.id) AS avg_rating,
        COUNT(r.id) OVER (PARTITION BY dn.id) AS review_count,
        SUM(r.usefulCount) OVER (PARTITION BY dn.id) AS total_useful_count
    FROM 
        drugNames dn
    JOIN 
        reviews r ON dn.id = r.drug_id
    JOIN 
        metadata m ON dn.id = m.drug_id
    ORDER BY 
        dn.drugName, m.date DESC;
    '''
    start_time = time.perf_counter()
    cursor.execute(query)
    end_time = time.perf_counter()
    rows = cursor.fetchall()

    row_count = len(rows)
    conn.commit()
    conn.close()
    return {'Element count': row_count, 'SQLite Time': end_time - start_time}


def advancedTestChroma(collection: chromadb.Collection) -> dict[str, int | float]:
    where_filter = {
        "$and": [
            {
                "$or": [
                    {
                        "rating": {"$lte": 5}
                    },
                    {
                        "usefulCount": {"$gte": 4}
                    },
                ],
            },
            {
                'condition': {"$ne": 'Depression'}

            }
        ]
    }

    where_document_filter = {
        "$contains": "I"
    }

    start_time = time.perf_counter()
    query_results = collection.query(
        query_texts=random.choices(possibleQueries),
        n_results=10_000,
        where=where_filter,
        where_document=where_document_filter
    )
    end_time = time.perf_counter()
    return {'Element count': len(query_results['metadatas'][0]), 'Chroma Time': end_time - start_time}


def make_charts(data: list):
    chroma_simple = data[0]
    SQLite_simple = data[1]
    chroma_advanced = data[2]
    SQLite_advanced = data[3]
    chroma_quantity = data[4]
    simple_ys = [[], []]
    simple_x = []
    advanced_ys = [[], []]
    advanced_x = []
    quantity_y = []
    quantity_x = []
    idx = 1
    for c, s in zip(chroma_simple, SQLite_simple):
        simple_ys[0].append(s['SQLite Time'])
        simple_ys[1].append(c['Chroma Time'])
        simple_x.append(idx)
        idx += 1
    make_chart("Proste wyszukiwanie",
               "Numer próby",
               "Czas[s]",
               simple_x,
               simple_ys,
               'linear',
               'log')
    for c, s in zip(chroma_advanced, SQLite_advanced):
        advanced_ys[0].append(s['SQLite Time'])
        advanced_ys[1].append(c['Chroma Time'])
        advanced_x.append(idx)
        idx += 1
    make_chart("Skomplikowane wyszukiwanie",
               "Numer próby",
               "Czas[s]",
               advanced_x,
               advanced_ys,
               'linear',
               'log')
    for c in chroma_quantity:
        quantity_y.append(c['Chroma Time'])
        quantity_x.append(c['Element count'])
    make_chart_simple("Czas dla wyszukiwania w chroma zależności od ilości wyszukiwanych elementow",
                      "Ilość elemetów",
                      "Czas[s]",
                      quantity_x,
                      quantity_y,
                      'log')


def Chroma_select_test_C(collection: chromadb.Collection, options: list[int]) -> list[dict[str, int | float]]:
    c_res = []
    for option in options:
        start_time = time.perf_counter()
        collection.query(query_texts=random.choices(possibleQueries),
                         n_results=option)
        end_time = time.perf_counter()
        c_res.append({'Element count': option, 'Chroma Time': end_time - start_time})
    return c_res


def describe_test() -> None:
    print("--------------------------------------------------")
    print("-------------------SELECT TEST--------------------")
    print("-Ten test upewnia się, że tabele/kolekcje mają odpowiednij rozmiar")
    print("-Wykonuje 3 rodzaje testów")
    print("-(A)proste zapytanie do obu baz")
    print("-(B)skomplikowane zapytanie do obu baz")
    print("-(C)proste zapytanie do chroma o różną ilość dancy")
    print("-Dla obu testów dla chroma losowany jest tekst zapytania")
    print("z listy od bardzo długich (950 znaków) po puste zapytanie")
    print("-Wyniki są zbierane i zapisywane do pliku select_results.json")
    print("-Na podstawie wyników tworzone są wykresy")
    print("--------------------------------------------------")


def perform_select_test() -> None:
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
    chroma_A_res = []
    chroma_B_res = []
    SQLite_A_res = []
    SQLite_B_res = []
    print("Rozpoczęcie testu A")
    for i in range(5):
        print(f"Rozpoczęcie testu nr: {i + 1}")
        print(f"\rWyszukiwanie Chroma")
        chroma_A_res.append(simpleTestChroma(collection))
        print(f"\rWyszukiwanie SQLite")
        SQLite_A_res.append(simpleTestSQLite())
    print("Rozpoczęcie testu B")
    for i in range(5):
        print(f"Rozpoczęcie testu nr: {i + 1}")
        print(f"\rWyszukiwanie Chroma")
        chroma_B_res.append(advancedTestChroma(collection))
        print(f"\rWyszukiwanie SQLite")
        SQLite_B_res.append(advancedTestSQLite())
    print("Rozpoczęcie testu C")
    print(f"Wyszukiwanie Chroma")
    chroma_C_res = Chroma_select_test_C(collection, [1, 10, 50, 100, 1000, 10_000, 40_000])

    print("\rZapisywanie wyników do pliku")
    save_results_to_file('select_results.json', [chroma_A_res, SQLite_A_res, chroma_B_res, SQLite_B_res, chroma_C_res])
    print("\rTworzenie wykresów")
    make_charts([chroma_A_res, SQLite_A_res, chroma_B_res, SQLite_B_res, chroma_C_res])
    end_message()
