import csv
import json

#________________________________________________________________
#Este codigo crea el archivo vino.json
with open('winemag-data_first150k.csv', encoding='utf-8') as f:
    reader = csv.reader(f)
    next(reader)
    data=[]
    for row in reader:
        data.append({"PII":row[0],"country":row[1],"designation":row[3]})
   
   
with open('vino.json', 'w', encoding='utf-8') as f:
    json.dump(data, f,ensure_ascii=False, indent=4)

#___________________________________________________
#Este codigo crea el archivo idpo.json

with open('winemag-data_first150k.csv', encoding='utf-8') as f:
    reader = csv.reader(f)
    next(reader)
    data=[]
    for row in reader:
        data.append({"PII":row[0],"points":row[4],"price":row[5]})
   
   
with open('idpo.json', 'w', encoding='utf-8') as f:
    json.dump(data, f,ensure_ascii=False, indent=4)