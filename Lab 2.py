import random

# Task 1: Count records where Title field has a line longer than 30 characters
def count_long_titles(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        records = file.readlines()
        long_title_count = sum(1 for record in records if len(record.split(',')[0]) > 30)  # Assuming Title is the first field
    return long_title_count

# Task 2: Implement a book search by author
def search_books_by_author(filename, author_name, limit):
    with open(filename, 'r', encoding='utf-8') as file:
        records = [record for record in file.readlines() if author_name.lower() in record.lower()]
    return records[:limit]

# Task 3: Generate bibliographic references
def generate_bibliographic_references(filename, num_records=20):
    with open(filename, 'r', encoding='utf-8') as file:
        records = [record.strip() for record in file.readlines()]
        random_records = random.sample(records, min(num_records, len(records)))  # Select random records
        references = [f"{record.split(',')[1]}. {record.split(',')[0]} - {record.split(',')[2]}" for record in random_records]  # Assuming author, title, year
    with open('bibliographic_references.txt', 'w', encoding='utf-8') as ref_file:
        for index, reference in enumerate(references, 1):
            ref_file.write(f"{index}. {reference}\n")

# Task 4: Parse currency.xml and extract data
def parse_currency_xml(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        content = file.read().splitlines()
        tags = set()
        currency_dict = {}
        for line in content:
            if '<Valute>' in line:
                num_code = line.split('<NumCode>')[1].split('</NumCode>')[0]
                char_code = line.split('<CharCode>')[1].split('</CharCode>')[0]
                currency_dict[num_code] = char_code
            tags.update(line.split('<')[1:])  # Extract tags
        unique_tags = set(tag.split('>')[0] for tag in tags if '>' in tag)
    return unique_tags, {k: v for k, v in currency_dict.items() if int(k) <= 200}

# Usage
books_csv = 'books-en.csv'  # Adjust the file name as necessary
currency_xml = 'currency.xml'  # Adjust the file name as necessary

# Count long titles
long_titles_count = count_long_titles(books_en.csv)
print(f"Number of records with title longer than 30 characters: {long_titles_count}")

# Search for books by author
author_to_search = 'Some Author'  # Replace with the desired author name
limit = 5  # Adjust limit as necessary
found_books = search_books_by_author(books_csv, author_to_search, limit)
print(f"Books by {author_to_search}: {found_books}")

# Generate bibliographic references
generate_bibliographic_references(books_csv)

# Parse XML and extract data
unique_tags, currency_dict = parse_currency_xml(currency_xml)
print(f"Unique tags: {unique_tags}")
print(f"Currency Dictionary (NumCode - CharCode): {currency_dict}")
