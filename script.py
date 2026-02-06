import requests

from typing import Optional, Union

from sqlalchemy import create_engine, Column, INTEGER,String,VARCHAR, Numeric
from sqlalchemy.orm import declarative_base, sessionmaker, DeclarativeBase

from pydantic import BaseModel, ValidationError

from dotenv import load_dotenv

from datetime import date

from country_converter import CountryConverter

import os

import time

load_dotenv()

DB_FILE = "trade_data.db"

engine = create_engine(f'sqlite:///{DB_FILE}')

Base:DeclarativeBase = declarative_base()

coco = CountryConverter()

def chunk_list(data_list, chunk_size):
    for i in range(0, len(data_list), chunk_size):
        yield data_list[i:i + chunk_size]

def get_valid_hs4_codes():
    url = "https://comtradeapi.un.org/files/v1/app/reference/HS.json"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        valid_codes = []
        results = data.get('results', [])
        
        print(f"   Total referensi ditemukan: {len(results)} item")
        
        for item in results:
            code = str(item.get('id'))
            
            if code.isdigit() and len(code) == 4:
                valid_codes.append(code)
        
        print(f"✅ Berhasil menyaring {len(valid_codes)} kode HS-4 yang valid.")
        return sorted(valid_codes)
        
    except Exception as e:
        print(f"❌ Gagal ambil referensi: {e}")
        return [str(i) for i in range(100, 9999) if len(str(i)) == 4]

class Trade(Base):
    __tablename__ = 'trade'
    id = Column(INTEGER,primary_key=True, autoincrement=True)

    kode_alpha3_reporter = Column(String(3),nullable=False)
    provinsi_reporter = Column(VARCHAR(255))
    kota_reporter = Column(VARCHAR(255))

    kode_alpha3_partner= Column(String(3), nullable= False)
    provinsi_partner = Column(VARCHAR(255))
    kota_partner= Column(VARCHAR(255))

    bulan= Column(VARCHAR(255), nullable = False)
    tahun= Column(VARCHAR(255), nullable=False)
    
    hscode= Column(VARCHAR(255),nullable=False)
    id_sector= Column(VARCHAR(255),nullable=False)
    
    vol= Column(VARCHAR(255))
    satuan= Column(VARCHAR(255))
    tarif = Column(Numeric(20,2))
    nilai= Column(Numeric(20,2))
    
    kode_sumber= Column(String(3),nullable=False)
    kode_flow = Column(String(1),nullable=False)

class TradeModel(BaseModel):
    reporterCode:Union[int,str] #kode_alpha3_reporter
    

    partnerCode:Union[int,str] #kode_alpha3_partner
   

    refMonth:Union[int,str] #bulan
    refYear:Union[int,str] #tahun

    classificationCode:str #hscode

    cmdCode :str #id_sector

    qty:Optional[Union[float,str]] = None #vol
    qtyUnitCode:Optional[Union[str,int]] = None #satuan --> ubah jadi kg/g
    primaryValue:Optional[Union[str,float]] = None #tarif
    netWgt:Optional[Union[str,float]] = None #nilai

    provinsi_reporter:Optional[str] = None
    kota_reporter:Optional[str] = None

    provinsi_partner:Optional[str] = None
    kota_partner:Optional[str] = None

    kode_sumber:str = '5'
    flowCode:str


Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

def countryConvertertoIso(m49_code) -> str:
    try:
        if str(m49_code) == '0': return 'WLD'
        result = coco.convert(names=str(m49_code), src='UNCode', to='ISO3')
        if isinstance(result,list):
            result = result[0]
        if len(result)>3:
            return str(m49_code)
        else:
            return result
    except:
        return m49_code

def countryConvertertoM49(iso_code) -> str:
    try:
        if str(iso_code) == '0': return '0'
        return coco.convert(names=str(iso_code), src='ISO3', to='UNCode')
    except:
        return None

def qtyUnitCodeConverter(unitCode):
    uc = str(unitCode)
    if uc == '8': return 'kg' 
    elif uc == '5': return 'u'
    else: return None

def get_data_trade_annual(reporter_code:str, partner_code:str, hs_code:str,tahun:str = date.today().year):
    GET_URL = 'https://comtradeapi.un.org/data/v1/get/C/A/HS?includeDesc=false'
    API_KEY = os.environ.get('API_KEY')
    params = {
        'reporterCode':reporter_code,

        'partnerCode':partner_code,

        'period':tahun,
        'format':'json',
        'freqCode':'A',
        'cmdCode':hs_code
    }
    header = {
        'Ocp-Apim-Subscription-Key': API_KEY
    }
    try:
        response = requests.get(GET_URL,params=params,headers=header)
        if response.status_code == 200:
            json_data:dict = response.json()
            return json_data.get('data',[])
        else:
            print(f'Gagal mengambil data! Status: {response.status_code}')
            print(f'Pesan: {response.text}')
            return []
    except Exception as e:
        print(f'Error Request: {e}')
        return []

def main():
    print('Masukan negara asal(USDN Code)*: ')
    rCodeInput = input().strip().upper()

    print('Masukan ke negara mana melakukan transaksi(USDN Code):')
    pCodeInput = input().strip().upper()

    print('Ingin memasukan data dari tahun berapa?*')
    sYearInput = input().strip()
    
    print('Ingin memasukan data sampai tahun berapa?(2025)')
    eYearInput = input().strip()

    avail_hs_codes = get_valid_hs4_codes()

    if not eYearInput: eYearInput = '2025'

    try:
        s_year = int(sYearInput)
        e_year = int(eYearInput)
    except ValueError:
        print("Error: Tahun harus berupa angka.")
        return

    rCodeM49 = countryConvertertoM49(rCodeInput)
    pCodeM49 = countryConvertertoM49(pCodeInput) if pCodeInput else None
    
    if not rCodeM49:
        print('Error: Kode negara asal tidak valid')
        return
    
    total_data_saved = 0

    for curr_year in range(s_year,e_year+1):
        year_str = str(curr_year)
        year_success = 0
        print(f'\nMEMPROSES TAHUN {year_str}')

        BATCH_SIZE = 20

        batches = list(chunk_list(avail_hs_codes,BATCH_SIZE))

        for i,batch in enumerate(batches):
            hs_code_str = ",".join(batch)
            batch_success = 0
            print(f'Batch {i+1}/{len(batches)}(HS {batch[0]}-{batch[-1]})')
            raw_data = get_data_trade_annual(rCodeM49,pCodeM49,hs_code_str,year_str)
            if not raw_data:
                print('Tidak ada data yang diambil')
                time.sleep(1)
                continue
            print('Mulai validasi data')
            for item in raw_data:
                try:
                    clean_item = TradeModel(**item)
                    db_row = Trade(
                        kode_alpha3_reporter = countryConvertertoIso(clean_item.reporterCode), #konversi
                        provinsi_reporter = clean_item.provinsi_reporter,
                        kota_reporter = clean_item.kota_reporter,
                        kode_alpha3_partner= countryConvertertoIso(clean_item.partnerCode), #konversi
                        provinsi_partner = clean_item.provinsi_partner,
                        kota_partner= clean_item.kota_partner,
                        bulan= str(clean_item.refMonth),
                        tahun= str(clean_item.refYear),
                        hscode= clean_item.classificationCode,
                        id_sector= clean_item.cmdCode,
                        vol= str(clean_item.qty),
                        satuan= qtyUnitCodeConverter(clean_item.qtyUnitCode), #konversi
                        tarif = clean_item.primaryValue,
                        nilai= clean_item.netWgt,
                        kode_sumber= clean_item.kode_sumber,
                        kode_flow = clean_item.flowCode
                    )
                    session.add(db_row)
                except ValidationError as e:
                    print(f'Data ditolak pydantic: {e}')
                except Exception as e:
                    print(f'Error Databse: {e}')
                batch_success += 1
            session.commit()
            year_success += batch_success
        total_data_saved += year_success
        print(f'\nTAHUN {year_str} SELESAI! {year_success} data berhasil disimpan')
    print(f'\n SELESAI! {total_data_saved} data berhasil disimpan')
    session.close()

if __name__ == '__main__':
    main()