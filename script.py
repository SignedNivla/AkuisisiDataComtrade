from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import requests

from typing import Any, Optional, Union

from sqlalchemy import CHAR, Integer, create_engine, Column,String, Numeric, select
from sqlalchemy.orm import declarative_base, sessionmaker, DeclarativeBase, Session

from pydantic import BaseModel, Field, ValidationError, field_validator

from dotenv import load_dotenv

from datetime import date

import logging

from country_converter import CountryConverter

import os

import time

from sqlalchemy import event
# ---CONFIG & SETUP---

load_dotenv()
coco = CountryConverter()

# --DATABASE--
DB_FILE = "trade_data.db"
engine = create_engine(f'sqlite:///{DB_FILE}')
Base:DeclarativeBase = declarative_base()

@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.close()


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
        
        if key == 'WLD':
            result = '0'

        if key == 'ALL': 
            return 'ALL'

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

class Trade(Base):
    __tablename__ = 'tbtrade'
    __tablename__ = 'trade'
    
    # 1. ID (INT 11, PK, Auto Increment)
    id = Column(Integer, primary_key=True, autoincrement=True)

    # 2. Kode_Alpha3_Reporter (CHAR 3) - Ada ikon kunci (Index)
    kode_alpha3_reporter = Column(CHAR(3), nullable=True, index=True)

    # 3. Provinsi_Reporter (VARCHAR 255)
    provinsi_reporter = Column(String(255), nullable=True)

    # 4. Kota_Reporter (VARCHAR 255)
    kota_reporter = Column(String(255), nullable=True)

    # 5. Kode_Alpha3_Partner (CHAR 3) - Ada ikon kunci (Index)
    kode_alpha3_partner = Column(CHAR(3), nullable=True, index=True)

    # 6. Provinsi_Partner (VARCHAR 255)
    provinsi_partner = Column(String(255), nullable=True)

    # 7. Kota_Partner (VARCHAR 255)
    kota_partner = Column(String(255), nullable=True)

    # 8. Bulan (VARCHAR 15)
    bulan = Column(String(15), nullable=True)

    # 9. Tahun (YEAR) - Di SQLite/Python kita pakai Integer atau String(4)
    # Ada ikon kunci (Index)
    tahun = Column(Integer, nullable=True, index=True) 

    # 10. HsCode (VARCHAR 9) - Ada ikon kunci (Index)
    hscode = Column(String(9), nullable=True, index=True)

    # 11. ID_Sektor (VARCHAR 15) - Ada ikon kunci (Index)
    id_sektor = Column(String(15), nullable=True, index=True)

    # 12. Vol (INT 11)
    vol = Column(Integer, nullable=True)

    # 13. Satuan (VARCHAR 25)
    satuan = Column(String(25), nullable=True)

    # 14. Tarif (DECIMAL 45,2) - Default '0.00'
    tarif = Column(Numeric(45, 2), nullable=True, default=0.00)

    # 15. Nilai (DECIMAL 45,2)
    nilai = Column(Numeric(45, 2), nullable=True)

    # 16. Kode_Sumber (CHAR 2) - Ada ikon kunci (Index)
    kode_sumber = Column(CHAR(2), nullable=True, index=True)

    # 17. Status (VARCHAR 19) - Ada ikon kunci (Index)
    status = Column(String(19), nullable=True, index=True)

    # 18. Berat_Bersih (DECIMAL 18,2)
    berat_bersih = Column(Numeric(18, 2), nullable=True)

    # 19. Pelabuhan (VARCHAR 150)
    pelabuhan = Column(String(150), nullable=True)

    # 20. hs_len (TINYINT 3) - Di Python jadi Integer
    # Ada ikon kunci (Index)
    hs_len = Column(Integer, nullable=True, index=True)
# ---DATA CLEANING LAYER---

class TradeModel(BaseModel):
    # Field Wajib dengan Default Aman
    reporterCode: Union[int, str] = Field(default=0)
    partnerCode: Union[int, str] = Field(default=0)
    refYear: Union[int, str] = Field(default=0)
    cmdCode: str = Field(default="0000")
    flowCode: str = Field(default="X")
    
    # Field Optional (Bisa None dari API)
    classificationCode: Optional[str] = Field(default=None)
    refMonth: Optional[Union[int, str]] = Field(default=None)
    
    # Angka-angka (Sering Null/Empty String di API)
    qty: Optional[float] = Field(default=0.0)
    qtyUnitCode: Optional[Union[str, int]] = Field(default=None)
    primaryValue: Optional[float] = Field(default=0.0)
    netWgt: Optional[float] = Field(default=0.0)

    # Tambahan: Abaikan field aneh
    provinsi_reporter: Optional[str] = None
    kota_reporter: Optional[str] = None
    provinsi_partner: Optional[str] = None
    kota_partner: Optional[str] = None
    kode_sumber: str = '5'

    class Config:
        extra = 'ignore' 
        populate_by_name = True

    # --- VALIDATOR SAKTI ---
    @field_validator('qty', 'primaryValue', 'netWgt', mode='before')
    @classmethod
    def sanitize_nums(cls, v: Any) -> float:
        # 1. Handle NoneType (Penting! float(None) itu Error)
        if v is None: 
            return 0.0
        
        # 2. Handle String
        if isinstance(v, str):
            v = v.strip()
            # Cek string kosong atau "null" text
            if v == "" or v.lower() in ["null", "n/a", "nan"]: 
                return 0.0
            try:
                return float(v)
            except ValueError:
                return 0.0
        
        # 3. Handle Angka (Int/Float)
        return float(v)
    
# ---PROCESSING BATCH---

class TradeBatchProcessor:
    def __init__(self, session: Session, mapper: CountryCodeConverter):
        self.session = session
        self.mapper = mapper
        self.errors = []
    
    def process(self, raw_data_list: list[dict]) -> dict:
        batch_payload = []
        
        for index, item in enumerate(raw_data_list):
            row = self._transform(item, index) 
            if row:
                batch_payload.append(row)
        
        saved_count = 0
        if batch_payload:
            try:
                self.session.bulk_insert_mappings(Trade, batch_payload)
                self.session.commit()
                saved_count = len(batch_payload)
            except Exception as e:
                self.session.rollback()
                print(f"      ❌ DATABASE REJECT: {e}")
        else:
            if raw_data_list:
                print(f"      ⚠️ WARNING: {len(raw_data_list)} data ditarik, tapi 0 lolos. Cek log error di atas.")

        return {'total': len(raw_data_list), 'success': saved_count}

    def _transform(self, raw_item: dict, idx: int) -> dict:
        try:
            # 1. Bersihkan Data
            clean = TradeModel(**raw_item)
            
            # 2. Konversi Negara
            rep_iso = self.mapper.to_iso3(clean.reporterCode)
            part_iso = self.mapper.to_iso3(clean.partnerCode)

            # 3. SAFETY LOGIC (Penyebab utama crash)
            # Pastikan tidak pernah melakukan int(None)
            vol_safe = int(clean.qty) if clean.qty is not None else 0
            nilai_safe = clean.primaryValue if clean.primaryValue is not None else 0.0
            berat_safe = clean.netWgt if clean.netWgt is not None else 0.0
            
            bulan_safe = str(clean.refMonth).zfill(2) if clean.refMonth else None
            satuan_safe = str(clean.qtyUnitCode) if clean.qtyUnitCode else None
            hs_len_safe = len(str(clean.cmdCode)) if clean.cmdCode else 0

            return {
                'kode_alpha3_reporter' : rep_iso,
                'kode_alpha3_partner' : part_iso,
                'bulan' : bulan_safe,
                'tahun' : clean.refYear,
                'hscode' : clean.cmdCode,
                'id_sektor' : clean.classificationCode,
                
                'vol' : vol_safe,       
                'satuan' : satuan_safe, 
                'tarif' : 0.0,
                'nilai' : nilai_safe,   
                
                'kode_sumber' : clean.kode_sumber,
                'status' : 'Import' if clean.flowCode == 'M' else 'Export',
                'berat_bersih' : berat_safe,
                'pelabuhan' : None,
                'hs_len' : hs_len_safe,
                
                'provinsi_reporter' : clean.provinsi_reporter,
                'kota_reporter' : clean.kota_reporter,
                'provinsi_partner' : clean.provinsi_partner,
                'kota_partner' : clean.kota_partner,
            }

        except Exception as e:
            # INI YANG PENTING: Print errornya biar tau salah dimana
            print(f"      ❌ Transform Error (Data ke-{idx}): {e}")
            return None


# ---UTILS---
# --DEFINING DATAS PER CHUNK--

def chunk_list(data_list, chunk_size):
    for i in range(0,len(data_list),chunk_size):
        yield data_list[i:i+chunk_size]

# --REQUESTING API--

class ComtradeClient:
    '''
    Client khusus untuk menangani komunikasi dengan UN Comtrade API
    '''
    BASE_URL = 'https://comtradeapi.un.org/public/v1/getDA/C/A/HS'
    def __init__(self, api_key:str):
        self.session = HTTP_SESSION
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
        'includeDesc':'false',
        'flowCode':flow_code
        }
        headers = {
            'Ocp-Apim-Subscription-Key': self.api_key
        }

        if partner and partner.upper() != "ALL":
            params["partnerCode"] = partner

        try:
            response:requests.Response = self.session.get(self.BASE_URL,params=params,headers=headers)

            if response.status_code == 403:
                 logger.critical("⛔ QUOTA EXCEEDED (403). Script harus berhenti.")
                 raise ComtradeAPIError("403 Quota Exceeded")

            response.raise_for_status()

            data:dict = response.json()

            error_value = data.get('error')

            if error_value:
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
        
class JobOrchestrator:
    '''
    Class ini bertanggung jawab terhadap menjalankan keseluruhan logika aplikasi
    '''
    def __init__(self):
        Base.metadata.create_all(engine)
        self.SessionLocal = sessionmaker(bind=engine)
        self.country_converter = CountryCodeConverter()
        self.client = ComtradeClient(os.getenv('API_KEY'))

        self.all_hs_codes = self._get_valid_hs4_codes()

    def _get_valid_hs4_codes(self) -> list[str]:
        url = "https://comtradeapi.un.org/files/v1/app/reference/HS.json"
        
        try:
            response:requests.Response = HTTP_SESSION.get(url)
            response.raise_for_status()
            data:dict = response.json()
            
            results = data.get('results', [])
            
            print(f"   Total referensi ditemukan: {len(results)} item")
            
            return sorted([str(item['id']) for item in results if str(item['id']).isdigit() and len(str(item['id'])) == 4])
            
        except Exception as e:
            print(f"❌ Gagal ambil referensi: {e}")
            return [str(i) for i in range(100, 9999) if len(str(i)) == 4]
        
    def _get_existing_code(self, reporter:str, year:str)->set[str]:
        session = self.SessionLocal()
        try:
            stmt = select(Trade.id_sektor).where(
                Trade.kode_alpha3_reporter == self.country_converter.to_iso3(reporter),
                Trade.tahun == year
            ).distinct()
            result = session.execute(stmt).scalars().all()
            return set(result)
        except Exception as e:
            logger.error(f"Gagal cek existing data: {e}")
            return set()
        finally:
            session.close()

    def run(self, reporters_iso: list[str], partner_iso: str, start_year: int, end_year: int):
        r_code = []
        for r_iso in reporters_iso:
            r_code.append(str(self.country_converter.to_m49(r_iso)))
        r_str = ",".join(r_code)
        print(r_str)
        p_code = self.country_converter.to_m49(partner_iso) if partner_iso else None
        
        print(f"\n🚀 START REPORTER: {r_str} ({r_code}) -> PARTNER: {partner_iso}")

        for year in range(start_year, end_year + 1):
            year_str = str(year)
            
            # 1. SMART FILTER (Checkpoint)
            print(f"   🔎 Menganalisis Database untuk Tahun {year_str}...")
            existing_hs = self._get_existing_code(r_code, year_str)
            
            # Set Difference: Apa yang kita mau - Apa yang sudah ada
            target_hs = sorted(list(set(self.all_hs_codes) - existing_hs))
            
            if not target_hs:
                print(f"   ✅ Tahun {year_str} sudah LENGKAP ({len(existing_hs)} codes). Skip.")
                continue
            
            print(f"   📉 Data Existing: {len(existing_hs)}. Target Fetch: {len(target_hs)} codes.")

            # 2. BATCHING
            # Gunakan Batch Size 5 (Aman untuk China) atau 1 (Sangat Aman)
            BATCH_SIZE = 10
            chunks = [target_hs[i:i + BATCH_SIZE] for i in range(0, len(target_hs), BATCH_SIZE)]
            
            db_session = self.SessionLocal()
            processor = TradeBatchProcessor(db_session, self.country_converter)

            for i, batch in enumerate(chunks):
                hs_str = ",".join(batch)
                try:
                    # Fetch
                    raw = self.client.fetch_annual_data(r_code, p_code, hs_str, year_str)
                    
                    # Save
                    count = processor.process(raw)
                    print(f"      Batch {i+1}/{len(chunks)}: Fetched & Saved {count['success']} rows.")
                    
                    # Anti-Rate Limit Sleep
                    time.sleep(1.5) 

                except Exception as e:
                    print(f"      ❌ Batch {i+1} Gagal: {e}")
                    # Opsional: Break loop jika error Quota 403 biar gak spam
                    if "403" in str(e): 
                        print("      ⚠️ QUOTA HABIS. Berhenti.")
                        return 

            db_session.close()
        
def main():
    reporters_env =  os.getenv("REPORTER_CODE", "IDN")
    partner_env = os.getenv("PARTNER_CODE","ALL")
    start_year = int(os.getenv("START_YEAR", "2023"))
    end_year = int(os.getenv("END_YEAR", "2025"))
    
    reporters = [x.strip() for x in reporters_env.split(',')]
    
    orchestrator = JobOrchestrator()
    orchestrator.run(reporters, partner_env, start_year, end_year)

if __name__ == '__main__':
    main()