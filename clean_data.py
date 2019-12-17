import re

with open('150kWines.txt', 'r') as f, open('150Wines2.txt', 'w') as fo:
    for line in f:
        fo.write(re.sub(r'(?!(([^"]*"){2})*[^"]*$),', '', line))