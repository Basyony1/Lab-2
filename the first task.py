#lab 2  
#the  first task
from csv import reader

with open('books-en.csv', 'r', encoding='windows-1251') as csvfile:
    table = reader(csvfile, delimiter=';')
    count = 0 #label for number of names more than 30 characters
    for row in table:
        if len(row[1])>30:
            count +=1

    if count ==0:
        print('There are no books with titles longer than 30 characters.')
    else:
        print(f'Found {count} books with titles longer than 30 characters.')
