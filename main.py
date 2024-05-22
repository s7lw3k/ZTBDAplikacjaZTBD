from Helpers.DeleteTests import perform_delete_test
from Helpers.InsertTests import SQLite_test_insert, perform_insert_test, create_tables
from Helpers.SelectTests import perform_select_test
from Helpers.UpdateTests import perform_update_test
from Helpers.connection import check_chroma_connection, check_sqlite_connection

if __name__ == "__main__":
    check_chroma_connection()
    check_sqlite_connection()
    selected_option = None
    create_tables()
    print("--------------------------------------------------")
    print("-----------Aplikacja do testowania Baz------------")
    print("--------------ChromaDB oraz SQLite----------------")
    print("--------------Sylwester Wieczorek-----------------")
    print("--------------------------------------------------")
    print("-Testy prowadzone są jednoczenie na obu bazach")
    print("-Po wybraniu opcji zostanie pokazany opis testu")
    print("-Wykresy generują się automatycznie w folderze ./Charts")
    print("--------------------------------------------------")
    print("------UWAGA OPCJA (5) może zająć pare godzin------")
    print("--------------------------------------------------")
    while selected_option != "6":
        print("Wybierz interesujący Cię test:")
        print("INSERT(1) UPDATE(2) DELETE(3) SELECT(4) ALL(5) QUIT(6)")
        selected_option = None
        while selected_option != "1" and selected_option != "2" and selected_option != "3" and selected_option != "4" and selected_option != "5" and selected_option != "6":
            selected_option = input("Twój wybór: ")
        match(selected_option):
            case "1":
                perform_insert_test()
            case "2":
                perform_update_test()
            case "3":
                perform_delete_test()
            case "4":
                perform_select_test()
            case "5":
                perform_insert_test()
                perform_select_test()
                perform_update_test()
                perform_delete_test()
