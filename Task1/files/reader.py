import json
import xml.etree.ElementTree as ET

'''
A class for reading data from json and writing into json or xml
'''

class FileWorker:
    def __init__(self):
        self.count = 0

    def read(self, path: str,logg):
        try:
            with open(path) as f:
                logg.info(path+" read")
                return json.load(f)
        except:
            logg.exception("No such file: "+path)

    def write(self, file: str, logg, data: dict, fields: list):
        try:
            match file:
                case 'xml':
                    self.xml_write(data,fields)
                case 'json':
                    self.json_write(data, fields)
                    
                case _:
                    logg.warning("No such type of file: "+file)
        except:
            logg.exception("Something went wrong during writing")
            
    def json_write(self, data: dict, fields: list):
        self.count += 1
        to_json = []
        
        for item in data:
            temp ={}
            for i, value in enumerate(item):
                temp[f"{fields[i]}"] = value
            to_json.append(temp)

        with open(f'results/result{self.count}.json','w') as f:
            json.dump(to_json,f)
    
    def xml_write(self, data: dict, fields: list):
        self.count += 1
        to_xml = ET.Element("data")
        
        for item in data:
            entry = ET.SubElement(to_xml, "entry")
            for i, value in enumerate(item):
                ET.SubElement(entry, fields[i]).text = str(value)
                
        xml_tree = ET.ElementTree(to_xml)
        with open(f'results/result{self.count}.xml','wb') as f:
            xml_tree.write(f, encoding='utf-8', xml_declaration=True)
