from csv import reader
import random 

output = open('result.txt', 'w')

with open('books-en.csv', 'r', encoding='windows-1251') as csvfile:
    table = reader(csvfile, delimiter=';')
    table = list(table)
    for i in range(20):
        row = random.choice(table)
        output.write(f'{i+1}. {row[2]}. {row[1]} - {row[3]}\n')
            
output.close()
print('20 records generated')