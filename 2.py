#lab2 the second task

from csv import reader

# Color codes for output
RED = '\u001b[41m'
END = '\u001b[0m'

# searching using the user

while True:
    count = 0
    request = input("Enter a request: ")
    if request.lower() == 'stop':
        print("Thank you for using!")
        break
    else:
        with open('books-en.csv', 'r', encoding='windows-1251') as csvfile:
            file = reader(csvfile, delimiter=';')
            next(file)  
            for row in file:
                lower_case = row[2].lower()  
                index = lower_case.find(request.lower())
                if index != -1:
                    if 1997 <= int(row[3]) <= 2000:  
                        count += 1
                        print(
                            f"{row[1]} "
                            f"(Author: {row[2]})"
                        )
                    else:
                        print(
                            f"{row[1]} "
                            f"(Author: {row[2]})\n   "
                            f"Status: {RED}Access denied{END}: "
                            f"You can download only books dated to 1997-2000"
                        )

        
        if count == 0:
            print('Nothing found.')

