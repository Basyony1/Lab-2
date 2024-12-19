from csv import reader

#1 издательства без повторений
publisher = set()

with open('books-en.csv', 'r', encoding='windows-1251') as csvfile:
    table = list(reader(csvfile, delimiter=';'))
    table.pop(0)
    for row in table:
        publisher.add(row[4])
    print(publisher)

    #2 самые популярные 20 книг
    popular = []
    for row in table:
        book = row[1]
        count = int(row[5])
        popular.append((count, book))
    popular = sorted(popular)[::-1]
    for num in range(20):
        print(f'{num+1}. {popular[num][1]}')