from numpy import concatenate
import requests
import re
import pandas as pd
import xml.etree.ElementTree as et 
import gspread
from gspread_dataframe import set_with_dataframe

def find_url(codigo_pais):
    return 'http://tarea-4.2021-1.tallerdeintegracion.cl/gho_'+codigo_pais+'.xml'

def parse_XML(xml_file, df_cols): 
    xtree = et.parse(xml_file)
    xroot = xtree.getroot()
    rows = []
    for node in xroot: 
        res = []
        for el in df_cols: 
            if node is not None and node.find(el) is not None:
                var = node.find(el).text
                if el == "Numeric":
                    str(var)
                    var = re.sub('[.]',',', var)
                res.append(var)
            else: 
                res.append(None)
        rows.append({df_cols[i]: res[i] 
                     for i, _ in enumerate(df_cols)})
    
    out_df = pd.DataFrame(rows, columns=df_cols)
        
    return out_df

df_cols =['GHO', 'COUNTRY', 'SEX', 'YEAR', 'GHECAUSES', 'AGEGROUP', 'Display', 'Numeric', 'Low', 'High']

####### C H I L E ########
URL_CLP = find_url("CHL")
data_clp = requests.get(URL_CLP)
with open('CHL.xml', 'wb') as file:
    file.write(data_clp.content)

output_xml_chl = parse_XML('CHL.xml', df_cols)

####### E S P A Ñ A  ########
URL_ESP = find_url("ESP")
data_esp = requests.get(URL_ESP)
with open('ESP.xml', 'wb') as file:
    file.write(data_esp.content)

output_xml_esp = parse_XML('ESP.xml', df_cols)

####### C A N A D A  ########
URL_CAN = find_url("CAN")
data_can = requests.get(URL_CAN)
with open('CAN.xml', 'wb') as file:
    file.write(data_can.content)

output_xml_can = parse_XML('CAN.xml', df_cols)

####### E S T A D O S   U N I D O S  ########
URL_USA = find_url("USA")
data_usa = requests.get(URL_USA)
with open('USA.xml', 'wb') as file:
    file.write(data_usa.content)

output_xml_usa = parse_XML('USA.xml', df_cols)

#######  B R A S I L  ########
URL_BRA = find_url("BRA")
data_bra = requests.get(URL_BRA)
with open('BRA.xml', 'wb') as file:
    file.write(data_bra.content)

output_xml_bra = parse_XML('BRA.xml', df_cols)

#######  C H I N A  ########
URL_CHN = find_url("CHN")
data_chn = requests.get(URL_CHN)
with open('CHN.xml', 'wb') as file:
    file.write(data_chn.content)

output_xml_chn = parse_XML('CHN.xml', df_cols)

output = pd.concat([output_xml_chl, output_xml_can, output_xml_bra, output_xml_chn, output_xml_esp, output_xml_usa])

# ACCES GOOGLE SHEET
gc = gspread.service_account(filename='taller-tarea-4-316718-091ed2e3b116.json')
sh = gc.open_by_key('1A3NeS8KQyYubvwubBXUf3GqOYWOowfenyWsNwzoVlTc')
worksheet = sh.get_worksheet(0) #-> 0 - first sheet, 1 - second sheet etc. 

# APPEND DATA TO SHEET
#your_dataframe = pd.DataFrame()
set_with_dataframe(worksheet, output) 