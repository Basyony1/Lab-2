'''Module providing functions working with XML files'''

import xml.dom.minidom as minidom

# Open and read the XML file
with open('currency.xml', 'r', encoding='windows-1251') as xml_file:
    xml_data = xml_file.read()

# Parse the XML file
dom = minidom.parseString(xml_data)
dom.normalize()  # Optimize XML structure for processing

# Get all 'Valute' elements
elements = dom.getElementsByTagName('Valute')

# Initialize empty lists for CharCode and Value
char_codes = []
values = []

# Iterate through each 'Valute' element and extract CharCode and Value
for node in elements:
    char_code = ''
    value = ''

    for child in node.childNodes:
        if child.nodeType == 1:  # Check if it's an element node
            if child.tagName == 'CharCode':
                if child.firstChild.nodeType == 3:  # Text node
                    char_code = child.firstChild.data
            if child.tagName == 'Value':
                if child.firstChild.nodeType == 3:
                    # Convert the Value text to a float and handle comma as decimal point
                    value = float(child.firstChild.data.replace(',', '.'))

    # Append the extracted CharCode and Value to their respective lists
    if char_code and value:  # Only add if both CharCode and Value are found
        char_codes.append(char_code)
        values.append(value)

# Display the results
print("CharCodes:", char_codes)
print("Values:", values)