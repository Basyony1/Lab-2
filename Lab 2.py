# Lab 2
# Task 1: Count records where Title field has a line longer than 30 characters
from csv import reader

with open('books-en.csv', 'r', encoding='windows-1251') as csvfile:
    table = reader(csvfile, delimiter=';')
    
    # Using list comprehension to count titles longer than 30 characters
    long_titles_count = sum(1 for row in table if len(row[1]) > 30)

    if long_titles_count == 0:
        print('No books with titles longer than 30 characters.')
    else:
        print(f'Found {long_titles_count} books with titles longer than 30 characters.')

# Task 2: Implement a book search by author
from csv import reader

def search_books(query):
    flag = 0  # label for quantity by search with limitation
    with open('books-en.csv', 'r', encoding='windows-1251') as csvfile:
        table = reader(csvfile, delimiter=';')
        for row in table:
            lower_case_title = row[2].lower()
            index = lower_case_title.find(query.lower())
            row[6] = row[6].replace(',', '.')  # Replace comma with dot for float conversion
            
            if index != -1 and float(row[6]) < 200:
                print(row[1])
                flag += 1

    return flag

while True:
    search = input('Enter query: ')
    if search == '0':
        break
    
    results_count = search_books(search)

    if results_count == 0:
        print('Nothing found.')
    else:
        print(f'Found {results_count} results.')

# Task 3: Generate bibliographic references
from csv import reader
import random

# Read data from the CSV file
with open('books-en.csv', 'r', encoding='windows-1251') as csvfile:
    table = list(reader(csvfile, delimiter=';'))

# Select 20 random rows
random_rows = random.sample(table, 20)

# Write selected rows to the output file
with open('result.txt', 'w') as output:
    for i, row in enumerate(random_rows, start=1):
        output.write(f'{i}. {row[2]}. {row[1]} - {row[3]}\n')

print('20 records generated')

# Task 4: Parse currency.xml and extract data
import xml.dom.minidom as minidom
from csv import reader

def parse_currency(xml_file):
    with open(xml_file, 'r') as file:
        xml_data = file.read()
        
    dom = minidom.parseString(xml_data)
    dom.normalize()

    elements = dom.getElementsByTagName('Valute')
    currency_dict = {}

    for node in elements:
        numcode = charcode = None
        for child in node.childNodes:
            if child.nodeType == 1:
                if child.tagName == 'NumCode' and child.firstChild.nodeType == 3:
                    numcode = child.firstChild.data
                elif child.tagName == 'CharCode' and child.firstChild.nodeType == 3:
                    charcode = child.firstChild.data
        
        if numcode and charcode:
            currency_dict[numcode] = charcode

        if node.getAttribute('id') == 'bk106':
            print(node.getElementsByTagName('NumCode')[0].firstChild.data)

    return currency_dict

def get_unique_publishers(csv_file):
    publisher = set()
    with open(csv_file, 'r', encoding='windows-1251') as file:
        table = list(reader(file, delimiter=';'))
        table.pop(0)  # Remove header
        for row in table:
            publisher.add(row[4])
    return publisher

def get_most_popular_books(csv_file, n=20):
    popular = []
    with open(csv_file, 'r', encoding='windows-1251') as file:
        table = list(reader(file, delimiter=';'))
        table.pop(0)  # Remove header
        for row in table:
            book = row[1]
            count = int(row[5])
            popular.append((count, book))
    
    # Sort by count descending
    popular = sorted(popular, key=lambda x: x[0], reverse=True)
    return popular[:n]

# Main execution
currency_dict = parse_currency('currency.xml')
for key, value in currency_dict.items():
    print(key, value)

unique_publishers = get_unique_publishers('books-en.csv')
print(unique_publishers)

most_popular_books = get_most_popular_books('books-en.csv')
for num, (count, book) in enumerate(most_popular_books, start=1):
    print(f'{num}. {book}')


import dask.dataframe as dd

# Load books-en.csv using Dask
books_en_df = dd.read_csv('books-en.csv')

# For unique publishers
unique_publishers = books_en_df['Publisher'].unique().compute()

print("Unique Publishers:")
for publisher in unique_publishers:
    print(publisher)

# For most popular books
# Assuming 'Popularity' is a column in your dataset
most_popular_books = books_en_df.nlargest(20, 'Popularity').compute()

print("\nMost Popular 20 Books:")
for index, row in most_popular_books.iterrows():
    print(f"{row['Title']} by {row['Author']} - Popularity: {row['Popularity']}")
