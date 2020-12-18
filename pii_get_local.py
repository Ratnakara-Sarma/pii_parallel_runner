import time
model_time = time.time()
import docx
import os
import pandas as pd
import sys

#######################################################################
from presidio_analyzer import AnalyzerEngine
from presidio_analyzer.nlp_engine import SpacyNlpEngine
from presidio_analyzer.recognizer_registry import RecognizerRegistry

nlp = SpacyNlpEngine({"en": "en_core_web_lg"})
analyzer = AnalyzerEngine(nlp_engine=nlp, default_language="en")
#######################################################################
print("\n")
print(f"took {time.time() - model_time} seconds to import libs & compile model")
print("\n")

src = f"D:\Datasets\Vishal\PII_mining\PII_API_testing"
files_list = os.listdir(src)

#######################################################################
def getText_Docx(filename):
    doc = docx.Document(filename)
    fullText = []
    for para in doc.paragraphs:
        fullText.append(para.text)
    return '\n'.join(fullText)

def entity_counter(entity_list):
    freq = {}
    entity_list = sorted(entity_list)
    for items in entity_list: 
        freq[items] = entity_list.count(items) 

    counter = ""
    for key, value in freq.items(): 
        if key == entity_list[-1]:
            counter += f"{key}:{value}"
        else:
            counter += f"{key}:{value}"+", "
    return counter


def sampler(lis, option):
    if option == '1':
        return(lis[0:25])
    elif option == '2':
        return(lis[25:50])
    elif option == '3':
        return(lis[50:75])
    elif option == '4':
        return(lis[75:100])
    elif option == '5':
        return lis
#######################################################################

if len(sys.argv)>1:
    option = sys.argv[1]
else:
    option = '5'

lis = sampler(files_list,option)
dataFrame = pd.DataFrame(columns = ['fileName', 'has_PII', 'PII_info']) 

source = f"D:\Datasets\Vishal\PII_mining\PII_API_testing"
print(f"Input dir: {source}")
print("\n")
print(f"set of files:")
print(lis)
start_time = time.time()

for file in lis:
    text = getText_Docx(os.path.join(source,file))
    response = analyzer.analyze(correlation_id=0,
                                text = text,
                                entities=[],
                                language='en',
                                all_fields=True)

    predicted_entities = []
    for item in response:
        predicted_entities.append(item.entity_type)
    if len(predicted_entities) == 0:
        haspii = "no"
        lenPII = 0
        PIIentry = "No PII found"
    else:
        haspii = "yes"
        lenPII = len(predicted_entities)
        PIIentry = entity_counter(predicted_entities)
    
    dataFrame = dataFrame.append({"fileName":str(file), "has_PII":str(haspii), "PII_info":str(PIIentry)},
                                     ignore_index=True)

if len(sys.argv)>1:
    num = sys.argv[1]
else:
    num = ""

print(f"writing csv as set_{num}.csv...")
dataFrame.to_csv("set_"+num+".csv")
print("Done")
print("\n")
print(f"took {time.time()-start_time} seconds to process files")