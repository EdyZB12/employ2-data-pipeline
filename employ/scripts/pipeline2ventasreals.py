import pandas as pd
import numpy as np 
import csv
import logging
import os 
from sqlalchemy import create_engine
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/pipeline.log'),
        logging.StreamHandler()
    ]
)

datos = "/app/data/employ_salary.csv"
limpios = "/app/output/employlimpio2.csv"

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'TMHzabusito88'),
    'database': os.getenv('DB_NAME', 'employ_data')
}


def pipeline(datos, limpios):
    try: 
        df = pd.read_csv(datos)
        print(df.head())
        print(df.info())


        ###############################
        #transform 
        ###############################

        #conversion de tipo: 
        col_numerica = ['COMPRATE', 'GRADE', 'OBJECTID']
        for col in col_numerica:
            df[col] = pd.to_numeric(df[col], errors = 'coerce')

        #identificamos datos faltantes

        miss_comprate = df['COMPRATE'].isnull().sum()
        if miss_comprate > 0: 
            logging.warning(f"Comprate is not valid: {miss_comprate}")


        miss_hire_date_string = df['HIREDATE_STRING'].isnull().sum()
        if miss_hire_date_string > 0: 
            logging.warning(f"Comprate is not valid: {miss_hire_date_string}")


        miss_jobitle = df['JOBTITLE'].isnull().sum()
        if miss_jobitle > 0: 
            logging.warning(f"Comprate is not valid: {miss_jobitle}")

        #duplicados OBJECTID     

        duplicados = df.duplicated(subset=['OBJECTID']).sum()
        if duplicados > 0:
            logging.warning(f"Duplicados en OBJECTID: {duplicados}")

        #fechas 

        df["HIREDATE_STRING"] = pd.to_datetime(
            df["HIREDATE_STRING"], 
            errors = "coerce",
        )

        miss_date = df["HIREDATE_STRING"].isna().sum()
        if miss_date > 0: 
            logging.warning(f"fechas no válidad detectadas: {miss_date}")


        df["HIREDATE_STRING"] = df["HIREDATE_STRING"].dt.strftime("%Y-%m-%d")

        df["HIRE_DATE"] = df["HIREDATE_STRING"]


        CP = df["COMPRATE"]

        def criterios_clasificacion(CP):
            if CP < 100: return 'Per hour'
            elif CP > 10000: return 'Annual'
            else: 
                return 'Unknown'
        
        df['SALARY_TYPE'] = df['COMPRATE'].apply(criterios_clasificacion)


        def calcular_conversion(CP):
            if CP < 100: 
                 return CP*2080 #horas anuales 
            elif CP >= 10000: 
                 return CP 
            else: 
                return CP 
            
        df['SALARY_ANNUAL'] = df['COMPRATE'].apply(calcular_conversion)

        #separación

        GTOA = df['GVT_TYPE_OF_APPT']

        partes = df['GVT_TYPE_OF_APPT'].str.split(' - ', expand = True)

        df['EMPLOYMENT_TYPE'] = partes[0]

        df['EMPLOYMENT_STATUS'] = partes[1]
        
        #mapeo de signficados

        def significados(GTOA):
            if 'CS' in GTOA: return 'Career Service'
            elif 'LS' in GTOA: return 'Legal Service'
            elif 'ExS' in GTOA: return 'Excepted Service'
            elif 'MSS' in GTOA: return 'Management Supervisory Service'
            elif 'Exe' in GTOA: return 'Executive'
            elif 'Ed' in GTOA: return 'Education Service'
            else:
                return 'No identification'
            
        df['Mean_of_GTV'] = df['GVT_TYPE_OF_APPT'].apply(significados) 
        
        #para entradas vacias en GRADE

        df['GRADE_CATEGORY'] = df['GRADE'].astype(str).fillna('Desconocido')

        #mapeo de significados GRADE

        GRA = df['GRADE']

        def sig_grade(GRA):
            GRA = str(GRA).split('.')[0]
            if GRA == '0' or GRA == '00': return 'Special Position'
            elif GRA in ['1', '2', '3', '4', '5', '6', '7', '8', '9']: return 'Entry Level'
            elif GRA in ['10', '11', '12', '13', '14', '15']: return 'Mid Level'
            elif GRA == '16': return 'Exclusive'
            elif GRA in ['E5', 'MD5', 'MD6', '3C', '5C']: return 'Senior Executive/Medical'
            else: 
                return 'Desconocido'

            
        df['gobernt_employe'] = df['GRADE'].apply(sig_grade)

        #mape de significados DESCRSHORT

        DS = df['DESCRSHORT']

        def sig_descrshort(DS):
            if 'DBH' in DS: return 'Department of Behavioral Health'
            elif 'DCHR' in DS: return 'Department of Human Resources'
            elif 'DCG' in DS: return 'Department of General Services'
            elif 'DCA' in DS: return 'Department of Consumer and Regulatory'
            elif 'CFSA' in DS: return 'Child and Family Service Agency'
            elif 'Council' in DS: return 'Council of the District of Columbia'
            elif 'CJCC' in DS: return 'Criminal Justice Coordinating Council'
            elif 'DDS' in DS: return 'Department of Disability Service'
            elif 'DDOT' in DS: return 'Distric Department of Transportation'
            elif 'DFS' in DS: return 'Department of Financial Service'
            elif 'MPD' in DS: return 'Metropolitian Police Department'
            elif 'DCPS' in DS: return 'Distric of Columbia Public Schools'
            elif 'UDC' in DS: return 'University of the District of Columbia'
            elif 'OAG' in DS: return 'Office of the Attorney General'
            elif 'OCFO' in DS: return 'Office of the Chief Financial Officer'
            elif 'DHS' in DS: return 'Department of Human Services'
            elif 'OSSE' in DS: return 'Office of the State Superintendent of Education'
            elif 'DOT' in DS: return 'Department of Transportation'
            elif 'DPW' in DS: return 'Department of Public Works'
            elif 'DOC' in DS: return 'Department of Corrections'
            elif 'DGS' in DS: return 'Department of General Services'
            elif 'DCRB' in DS: return 'District of Columbia Retirement Board'
            elif 'DLCP' in DS: return 'Department of Licensing and Consumer Protection'
            elif 'DMPED' in DS: return 'Deputy Mayor for Planning and Economic Development'
            elif 'DISB' in DS: return 'Department of Insurance, Securities and Banking'
            elif 'DOB' in DS: return 'Department of Buildings'
            elif 'ORM' in DS: return 'Office of Risk Management'
            elif 'DOH' in DS: return 'Department of Health'
            else: 
                return 'Desconocido'
        
        df['MEAN_DRESCSHORT'] = df['DESCRSHORT'].apply(sig_descrshort)

        #crea base de datos 

        def crear_base_dato():
            try:
                conn = psycopg2.connect(
                    host=DB_CONFIG['host'],
                    port=DB_CONFIG['port'],
                    user=DB_CONFIG['user'],
                    password = DB_CONFIG['password'],
                    dbname = 'postgres'
                ) 
                conn.autocommit = True
                cursor = conn.cursor()

                cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_CONFIG['database']}';")
                exists = cursor.fetchone()

                if not exists:
                    cursor.execute(f"CREATE DATABASE {DB_CONFIG['database']};")
                    print(f"Base de datos '{DB_CONFIG['database']}' creada exitosamente")
                else: 
                    print(f"La base de datos '{DB_CONFIG['database']}' ya exite")
                
                cursor.close()
                conn.close()
                
            except psycopg2.Error as e: 
                if "already exists" in str(e):
                    print(f"La base de datos{DB_CONFIG['database']} ya existe")
                else: 
                    print(f"Error creando base de datod: {e}")
        
        def crear_tablas():
            try: 
                conn = psycopg2.connect(**DB_CONFIG)
                cursor = conn.cursor()
                create_table_query = """
                CREATE TABLE IF NOT EXISTS employ_data(
                    id SERIAL PRIMARY KEY, 
                    pruid INTEGER, 
                    FIRST_NAME VARCHAR(100),
                    LAST_NAME VARCHAR(100),
                    JOBTITLE VARCHAR(100),
                    DESCRSHORT VARCHAR(100),
                    GRADE INTEGER, 
                    COMPRATE NUMERIC,
                    OBJECTID INTEGER, 
                    HIRE_DATE DATE,
                    SALARY_TYPE VARCHAR(100), 
                    SALARY_ANNUAL NUMERIC, 
                    EMPLOYMENT_TYPE VARCHAR(100),
                    Mean_of_GTV VARCHAR(100),
                    gobernt_employe VARCHAR(100)
                );
                """
                cursor.execute(create_table_query)
                cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_employ_data ON employ_data(HIRE_DATE);
                CREATE INDEX IF NOT EXISTS idx_employ_data ON employ_data(FIRST_NAME);
                """)

                conn.commit()
                print("Tablas e índices creados con existo")

            except psycopg2.Error as e: 
                print(f"Error creado tablas: {e}")
    
        def cargar_datos_posgres(df):
            try: 
                engine = create_engine(
                  f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
                  f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
                )

                df.to_sql(
                    name = 'employ_data',
                    con=engine, 
                    if_exists='replace',
                    index=False,
                    method='multi',
                    chunksize=1000
                )

                print("Datos cargados con exito a PosgreSQL")

            except Exception as e: 
               print(f"Error cargando datos a PosgreSQL: {e}")
               return False

        #print(df.dtypes)

        df.to_csv(limpios, index=False, encoding='utf-8')

        crear_base_dato()
        crear_tablas()
        cargar_datos_posgres(df)

        return True 

    except Exception as e:
        print(f"error: {e}")
        return False

if __name__ == "__main__":
    pipeline(datos, limpios)