from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import requests

from typing import Optional, Union

import sqlalchemy as db
from sqlalchemy.dialects import mysql
from sqlalchemy import create_engine, select
from sqlalchemy.orm import declarative_base, sessionmaker, Session

from pydantic import BaseModel, ValidationError, field_validator

from dotenv import load_dotenv

from datetime import date

import logging

from country_converter import CountryConverter

import os

import time
# ---CONFIG & SETUP---

load_dotenv()
coco = CountryConverter()

# --DATABASE--
DB_FILE = "trade_data.db"
engine = create_engine(os.getenv('TESTING_MY_SQL_URL',f'sqlite:///{DB_FILE}'),echo=True)
Base = declarative_base()

# @event.listens_for(engine, "connect")
# def set_sqlite_pragma(dbapi_connection, connection_record):
#     cursor = dbapi_connection.cursor()
#     cursor.execute("PRAGMA journal_mode=WAL")
#     cursor.execute("PRAGMA synchronous=NORMAL")
#     cursor.close()


# --REQUEST API DECLARATION--
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

class ComtradeAPIError(Exception):
    '''
    Error khusus jika API menolak request
    '''
    pass

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('extraction.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

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

        if key == 'ALL':
            return None

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
    
# ---DATABASE SCHEMA---

class TradeDB(Base):
    __tablename__ = 'tbtrade'

    id = db.Column(mysql.INTEGER(11),primary_key=True,autoincrement=True)
    kode_alpha3_reporter = db.Column(mysql.CHAR(3),nullable=True) #CHN
    provinsi_reporter = db.Column(mysql.VARCHAR(255),nullable=True) #Belum ada sampai saat ini(pastikan null)
    kota_reporter = db.Column(mysql.VARCHAR(255),nullable=True) #Belum ada sampai saat ini(pastikan null)
    kode_alpha3_partner = db.Column(mysql.CHAR(3),nullable=True) #
    provinsi_partner = db.Column(mysql.VARCHAR(255),nullable=True) #Belum ada sampai saat ini(pastikan null)
    kota_partner = db.Column(mysql.VARCHAR(255),nullable=True) #Belum ada sampai saat ini(pastikan null)
    bulan = db.Column(mysql.VARCHAR(15),nullable=True) #Belum ada sampai saat ini(pastikan null)
    tahun = db.Column(mysql.YEAR,nullable=True) #2021
    hscode = db.Column(mysql.VARCHAR(9),nullable=True) #911
    id_sektor = db.Column(mysql.VARCHAR(15),nullable=True) #Belum ada sampai saat ini
    vol = db.Column(mysql.BIGINT(),nullable=True) # 0/null
    satuan = db.Column(mysql.VARCHAR(25),nullable=True) #Belum ada sampai saat ini
    tarif = db.Column(mysql.DECIMAL(45,2),nullable=True,default=0.0) #Belum ada sampai saat ini(pastikan null)
    nilai = db.Column(mysql.DECIMAL(45,2),nullable=True) #11.0
    kode_sumber = db.Column(mysql.CHAR(2),nullable=True) #5
    status = db.Column(mysql.VARCHAR(19),nullable=True) #export/import
    berat_bersih = db.Column(mysql.DECIMAL(18,2),nullable=True) #1444220.0
    pelabuhan = db.Column(mysql.VARCHAR(150),nullable=True) #Belum ada sampai saat ini(pastikan null)
    hs_len = db.Column(mysql.TINYINT(3,unsigned = True)) #4

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
    classificationCode:str #id_sektor
    cmdCode :str #hscode
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
    def __init__(self, db_session:Session, mapper:CountryCodeConverter):
        self.db_session = db_session
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
                'kode_alpha3_reporter' : str(rep_iso),
                'kode_alpha3_partner' : str(part_iso),
                'bulan' : str(clean.refMonth).zfill(2) if clean.refMonth != None else None,
                'tahun' : str(clean.refYear) if clean.refYear != None else None,
                'hscode' : clean.cmdCode,
                'id_sektor' : None,
                'vol' : clean.qty if clean.qty != None else None,
                'satuan' : clean.qtyUnitCode,
                'tarif' : 0.0,
                'nilai' : clean.primaryValue,
                'kode_sumber' : clean.kode_sumber,
                'status' : clean.flowCode,
                'provinsi_reporter' : str(clean.provinsi_reporter) if clean.provinsi_partner != None else None,
                'kota_reporter' : str(clean.kota_reporter) if clean.kota_reporter != None else None,
                'provinsi_partner' : str(clean.provinsi_partner) if clean.provinsi_partner != None else None,
                'kota_partner' : str(clean.kota_partner) if clean.kota_partner != None else None,
                'berat_bersih': clean.netWgt if clean.netWgt != None else None,
                'pelabuhan': None,
                'hs_len':4
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
            self.db_session.bulk_insert_mappings(TradeDB,payload)
            self.db_session.commit()
            self.total_saved += len(payload)
        except Exception as e:
            self.db_session.rollback()
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
        response:requests.Response = HTTP_SESSION.get(url)
        response.raise_for_status()
        data:dict = response.json()
        
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

class ComtradeClient:
    '''
    Client khusus untuk menangani komunikasi dengan UN Comtrade API
    '''
    BASE_URL = 'https://comtradeapi.un.org/data/v1/get/C/A/HS'
    def __init__(self, db_session:Session, api_key:str):
        self.db_session = db_session
        self.api_key = api_key

        if not api_key:
            logger.warning('API Key tidak ditemukan!')
    
    def fetch_annual_data(self,reporter:str,partner:str,hs_codes:str,year:str,flow_code:str = 'M,X') ->list[dict]:
        params = {
        'reporterCode':reporter,
        'period':year,
        'cmdCode':hs_codes,
        'format':'json',
        'freqCode':'A',
        'includeDesc':'false'
        }
        headers = {
            'Ocp-Apim-Subscription-Key': self.api_key
        }

        if partner and partner.upper() != "ALL":
            params["partnerCode"] = partner

        try:
            response:requests.Response = self.db_session.get(self.BASE_URL,params=params,headers=headers)

            response.raise_for_status()

            data = response.json()

            if data.get('error') != "":
                raise ComtradeAPIError(f"API Error message: {data['error']}")
            
            results = data.get('data',[])
            logger.info(f"Fetched {len(results)} row for HS {hs_codes[:10]}...")
            
            return results
        
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                logger.error("Rate Limit Hitting Hard!")
            elif e.response.status_code == 401:
                logger.critical("API Key Invalid atau Expired!")
            raise ComtradeAPIError(f"HTTP failed:{e}")
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Connection Error: {e}")
            raise ComtradeAPIError(f"Network Failed: {e}")
            
        except ValueError:
            logger.error("Response is not valid JSON")
            raise ComtradeAPIError("Invalid JSON Response")

def get_existing_code(db_session:Session,reporter:str, year:str)->set[str]:
        try:
            stmt = select(TradeDB.hscode).where(
                TradeDB.kode_alpha3_reporter == reporter,
                TradeDB.tahun == year
            ).distinct()
            result = db_session.execute(stmt).scalars().all()
            return set(result)
        except Exception as e:
            logger.error(f"Gagal cek existing data: {e}")
            return set()

def main():
    # SETUP DB
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    convert_country = CountryCodeConverter()
    processor = TradeBatchProcessor(session,convert_country)
    api_client = ComtradeClient(HTTP_SESSION,API_KEY)

    reporters_env =  os.getenv("REPORTER_CODE", "IDN")
    partner_env = os.getenv("PARTNER_CODE","ALL")
    start_year = int(os.getenv("START_YEAR", "2023"))
    end_year = int(os.getenv("END_YEAR", "2025"))

    avail_hs_codes = get_valid_hs4_codes()

    if not end_year: end_year = '2025'

    try:
        s_year = int(start_year)
        e_year = int(end_year)
    except ValueError:
        print("Error: Tahun harus berupa angka.")
        return

    rCodeM49 = convert_country.to_m49(reporters_env)
    pCodeM49 = convert_country.to_m49(partner_env) if partner_env else None
    
    if not rCodeM49:
        print('Error: Kode negara asal tidak valid')
        return
    
    total_data_saved = 0

    for curr_year in range(s_year,e_year+1):
        year_str = str(curr_year)
        year_success = 0
        print(f'\nMEMPROSES TAHUN {year_str}')
        start_time = time.time()

        BATCH_SIZE = 500

        existing_hs = get_existing_code(session,reporters_env, year_str)
            
            # Set Difference: Apa yang kita mau - Apa yang sudah ada
        target_hs = sorted(list(set(avail_hs_codes) - existing_hs))
        
        if not target_hs:
            print(f"   ✅ Tahun {year_str} sudah LENGKAP ({len(existing_hs)} codes). Skip.")
            continue

        batches = list(chunk_list(target_hs,BATCH_SIZE))

        for i,batch in enumerate(batches):
            hs_code_str = ",".join(batch)
            print(f'Batch {i+1}/{len(batches)}(HS {batch[0]}-{batch[-1]})')
            
            raw_data = api_client.fetch_annual_data(rCodeM49,pCodeM49,hs_code_str,year_str)

            stats = processor.process(raw_data)

            print(f"Batch {i+1} Done. Saved: {stats['success']}/{stats['total']}. Errors: {len(processor.errors)}")
            year_success += stats['success']
            time.sleep(1.5)
        total_data_saved += year_success
        end_time = time.time()
        print(f'\nTAHUN {year_str} SELESAI dalam waktu {end_time-start_time}! {year_success} data berhasil disimpan')
    print(f'\n SELESAI! {total_data_saved} data berhasil disimpan')
    session.close()

if __name__ == '__main__':
    main()