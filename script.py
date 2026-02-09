from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import requests

from typing import Optional, Union

from sqlalchemy import create_engine, Column, INTEGER,String,VARCHAR, Numeric
from sqlalchemy.orm import declarative_base, sessionmaker, DeclarativeBase, Session

from pydantic import BaseModel, ValidationError, field_validator

from dotenv import load_dotenv

from datetime import date

from country_converter import CountryConverter

import os

import time

# ---CONFIG & SETUP---
load_dotenv()
coco = CountryConverter()
# CONSTANTS
DB_FILE = "trade_data.db"

def create_robust_session() -> Session:
    session = requests.Session()

    retry = Retry(
        total=5,
        backoff_factor=2,
        allowed_methods=['HEAD','GET','OPTIONS'],
        status_forcelist=[429,500,502,503,504]
    )

    adapter = HTTPAdapter(max_retries=retry)

    session.mount('https://',adapter=adapter)
    session.mount('http://',adapter=adapter)
    return session

HTTP_SESSION = create_robust_session()

API_KEY = os.environ.get('API_KEY')
engine = create_engine(f'sqlite:///{DB_FILE}')
Base:DeclarativeBase = declarative_base()

# ---CONVERTER CLASS---
class CountryCodeConverter:
    '''
    Class ini menangani konversi negara dengan Caching
    Hanya memanggil library 'CountryConverter' jika kode belum pernah dilihat sebelumnya.
    '''
    def __init__(self):
        print('Memulai Engine Country Converter...')
        self.coco:CountryConverter = CountryConverter()
        # Memory Cache {'InputCode':'OutputCode'}
        self._iso3_cache:dict[str,str] = {}
        self._m49_cache:dict[str,str] = {}
    
    def to_m49(self, iso_code) -> str:
        key = str(iso_code).strip()

        if key in self._m49_cache:
            return self._m49_cache[key]
        
        if key == 'WLD':
            result = '0'
        else:
            try:
                result = self.coco.convert(names = key,src = 'ISO3',to = 'UNCode')
            except Exception:
                result = None

        self._m49_cache[key] = result
        return result
        
    def to_iso3(self, m49_code) -> str:
        key = str(m49_code).strip()

        if key in self._iso3_cache:
            return self._iso3_cache[key]
        
        if key == '0':
            result = 'WLD'
        else:
            try:
                result = self.coco.convert(names = key,src = 'UNCode',to = 'ISO3')
                if len(result) > 3:result = key
            except Exception:
                result = key
                
        self._iso3_cache[key] = result
        return result

convert_country = CountryCodeConverter()

# ---DATABASE SCHEMA---
class Trade(Base):
    __tablename__ = 'trade'
    id = Column(INTEGER,primary_key=True, autoincrement=True)

    kode_alpha3_reporter = Column(String(3),nullable=False)
    kode_alpha3_partner= Column(String(3), nullable= False)

    bulan= Column(VARCHAR(10), nullable = False)
    tahun= Column(VARCHAR(4), nullable=False)
    
    id_sector= Column(VARCHAR(10),nullable=False)
    hscode= Column(VARCHAR(20),nullable=False)
    
    vol= Column(VARCHAR(255))
    satuan= Column(VARCHAR(255))
    tarif = Column(Numeric(20,2))
    nilai= Column(Numeric(20,2))
    
    kode_sumber= Column(String(3),nullable=False)
    kode_flow = Column(String(1),nullable=False)

    provinsi_reporter = Column(VARCHAR(255))
    kota_reporter = Column(VARCHAR(255))
    provinsi_partner = Column(VARCHAR(255))
    kota_partner= Column(VARCHAR(255))

# ---DATA CLEANING LAYER---
class TradeModel(BaseModel):
    '''
    Model ini bertugas 'membersihkan' data kotor dari API
    sebelum disentuh oleh logika database.
    '''
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

    @field_validator('qty','primaryValue','netWgt',mode='before')
    def sanitize_numbers(cls,v):
        if v == '' or v is None:
            return 0.0
        return float(v)
    
    @field_validator('qtyUnitCode',mode='before')
    def convert_unit(cls,v):
        v_str = str(v)
        if v_str == '8': return 'kg' 
        elif v_str == '5': return 'u'
        else: return None

# ---PROCESSING BATCH---
class TradeBatchProcessor:
    '''
    Class ini bertanggung jawab penuh atas siklus hidup
    pemrosesan satu batch data
    '''
    def __init__(self, session:Session, mapper:CountryCodeConverter):
        self.session = session
        self.mapper = mapper

        self.total_processed = 0
        self.total_saved = 0
        self.errors = []
    
    def process(self, raw_data_list:list[dict])->dict:
        '''
        Pintu masuk data. Mengembalikan ringkasan hasil kerja
        '''
        batch_payload = []
        for item in raw_data_list:
            self.total_processed += 1
            clean_row = self._transform(item)
            if clean_row:
                batch_payload.append(clean_row)
            else:
                pass
        
        if batch_payload:
            self._load_to_db(batch_payload)
        
        return{
            'total':self.total_processed,
            'success':self.total_saved,
            'failed':self.total_processed - self.total_saved,
            'sample_error': self.errors[0] if self.errors else None
        }

    def _transform(self, raw_item:dict)->dict:
        try:
            clean = TradeModel(**raw_item)
            rep_iso = self.mapper.to_iso3(clean.reporterCode)
            part_iso = self.mapper.to_iso3(clean.partnerCode)

            return {
                'kode_alpha3_reporter' : rep_iso,
                'kode_alpha3_partner' : part_iso,
                'bulan' : str(clean.refMonth).zfill(2),
                'tahun' : str(clean.refYear),
                'hscode' : clean.classificationCode,
                'id_sector' : clean.cmdCode,
                'vol' : str(clean.qty),
                'satuan' : clean.qtyUnitCode,
                'tarif' : clean.primaryValue,
                'nilai' : clean.netWgt,
                'kode_sumber' : clean.kode_sumber,
                'kode_flow' : clean.flowCode,
                'provinsi_reporter' : clean.provinsi_reporter,
                'kota_reporter' : clean.kota_reporter,
                'provinsi_partner' : clean.provinsi_partner,
                'kota_partner' : clean.kota_partner,
            }
        except ValidationError as e:
            self.errors.append(f'Validation Error: {str(e)}')
            return None
        except Exception as e:
            self.errors.append(f'Unexpected Error: {str(e)}')
            return None

    def _load_to_db(self, payload:list[dict]):
        '''
        Logika penyimpanan data dan interaksi dengan database
        '''
        try:
            self.session.bulk_insert_mappings(Trade,payload)
            self.session.commit()
            self.total_saved += len(payload)
        except Exception as e:
            self.session.rollback()
            self.errors.append(f'DB Insert Error:{str(e)}')
            print(f'CRITICAL: Batch failed to save! {e}')

# ---UTILS---
# --DEFINING DATAS PER CHUNK--
def chunk_list(data_list, chunk_size):
    for i in range(0,len(data_list),chunk_size):
        yield data_list[i:i+chunk_size]

# --GETTING ALL AVAILABLE HS4 CODE--
def get_valid_hs4_codes():
    url = "https://comtradeapi.un.org/files/v1/app/reference/HS.json"
    
    try:
        response = HTTP_SESSION.get(url)
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

# --REQUESTING API--
def get_data_trade_annual(reporter_code:str, partner_code:str, hs_code:str,tahun:str = date.today().year):
    GET_URL = 'https://comtradeapi.un.org/data/v1/get/C/A/HS?includeDesc=false'
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
        response = HTTP_SESSION.get(GET_URL,params=params,headers=header)
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
    # SETUP DB
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    processor = TradeBatchProcessor(session,convert_country)

    print('Masukan negara asal(ISO Code)*: ')
    rCodeInput = input().strip().upper()

    print('Masukan ke negara mana melakukan transaksi(ISO Code):')
    pCodeInput = input().strip().upper()

    print('Ingin memasukan data dari tahun berapa?*')
    sYearInput = input().strip()
    
    print('Ingin memasukan data sampai tahun berapa?(2025)')
    eYearInput = input().strip()

    print('Memulai proses akuisisi data!')

    avail_hs_codes = get_valid_hs4_codes()

    if not eYearInput: eYearInput = '2025'

    try:
        s_year = int(sYearInput)
        e_year = int(eYearInput)
    except ValueError:
        print("Error: Tahun harus berupa angka.")
        return

    rCodeM49 = convert_country.to_m49(rCodeInput)
    pCodeM49 = convert_country.to_m49(pCodeInput) if pCodeInput else None
    
    if not rCodeM49:
        print('Error: Kode negara asal tidak valid')
        return
    
    total_data_saved = 0

    for curr_year in range(s_year,e_year+1):
        year_str = str(curr_year)
        year_success = 0
        print(f'\nMEMPROSES TAHUN {year_str}')
        start_time = time.time()

        BATCH_SIZE = 20

        batches = list(chunk_list(avail_hs_codes,BATCH_SIZE))

        for i,batch in enumerate(batches):
            hs_code_str = ",".join(batch)
            print(f'Batch {i+1}/{len(batches)}(HS {batch[0]}-{batch[-1]})')

            raw_data = get_data_trade_annual(rCodeM49,pCodeM49,hs_code_str,year_str)

            stats = processor.process(raw_data)

            print(f"Batch {i+1} Done. Saved: {stats['success']}/{stats['total']}. Errors: {len(processor.errors)}")
            year_success += stats['success']
            time.sleep(1.5)
        total_data_saved += year_success
        end_time = time.time()
        print(f'\nTAHUN {year_str} SELESAI dalam waktu {start_time-end_time}! {year_success} data berhasil disimpan')
    print(f'\n SELESAI! {total_data_saved} data berhasil disimpan')
    session.close()

if __name__ == '__main__':
    main()