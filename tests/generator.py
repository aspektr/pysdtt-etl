if __name__ == '__main__':

    cursor = [{'a': '1'},
              {'b': '2'},
              {'c': '3'},
              {'d': '4'},
              {'e': '5'},
              {'f': '6'},
              {'g': '7'},
              {'h': '8'},
              {'i': '9'}]

    def f():
        list_rows = set()
        for i, row in enumerate(cursor):
            list_rows.add(row)
            if i%2 == 1:
                yield list_rows
                list_rows = set()
        yield list_rows

    for x in f():
        print(x)
