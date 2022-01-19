from subprocess import PIPE, Popen
import argparse
import cx_Oracle
import xml.etree.ElementTree as ET
from datetime import datetime
import random
import time
import os


class EElogEQTL(object):

    def __init__(self, db_user, db_password, db_dsn):
        super(EElogEQTL, self).__init__()
        self.version = "1.0v"
        self.filePath = os.path.dirname(__file__)
        self.fileName = "EElogEQTL.txt"
        self.fileLog = open("{}\\{}".format(self.filePath, self.fileName), 'a')
        try:
            self.registerLogPrint('__init__', 'Abrindo conexao com o banco ...')
            self.connection = cx_Oracle.connect(user=db_user, password=db_password, dsn=db_dsn)
            self.cursor = self.connection.cursor()
            self.registerLogPrint('__init__', 'Conexao estabelecida com sucesso!')
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            if error.code == 1:
                self.registerLogPrint('__init__', 'Erro ao conectar ao banco: ' + " " + str(error))
                raise

    def version(self):
        print(self.version)


    def registerLogPrint(self, func, text):
        text = "{} : {} => {} \n".format(datetime.now().strftime("%d-%m-%Y %H:%M:%S"), func, text)
        self.fileLog.write(text)
        print(text)


    def etl2xml(self, etlFile):
        """ Gera o .xml a partir de um arquivo .etl """
        t0 = time.time()
        with Popen("tracerpt {} -o EELOGEQTLBUFFER.xml -of XML -lr -y".format(etlFile), stdout=PIPE, stderr=PIPE) as p:
            output, errors = p.communicate()
            output = str(output)
            errors = str(errors)
            print(output)
            self.registerLogPrint('etl2xml', errors + " Erros")
        self.registerLogPrint('etl2xml', 'Convertido em : ' + str(time.time() - t0))


    def listFilesElt(self):
        results = self.cursor.execute("""SELECT LOGFILENAME FROM EELOG_CONTROLE""")
        names = [x[0] for x in self.cursor.description]
        listFileDirectory = [f for f in os.listdir() if f.endswith('.etl')]
        listFileDB = list(map(lambda x: x[0], results.fetchall()))
        return list(set(listFileDirectory) ^ set(listFileDB))


    def closeDB(self):
        self.connection.close()
        self.registerLogPrint('closeDB', 'Conexao do Banco de Dados finalizada com sucesso!')


    def closeFileLog(self):
        self.registerLogPrint('closeFileLog', 'Arquivo Log finalizada com sucesso!\n##############################################################################')
        self.fileLog.close()


    def insertLog(self, file):
        self.registerLogPrint('insertLog', '\n\nProcessando o arquivo: ' + str(file))
        self.etl2xml(file)
        t0 = time.time()
        tree = ET.parse("EELOGEQTLBUFFER.xml")
        root = tree.getroot()

        for child in root:
            try:
                binaryEventData = child[1]
                child = child[0]
                timeCreated = child[7].attrib
                execution = child[9].attrib
                processId = execution.get('ProcessID')
                threadID = execution.get('ThreadID')
                systemTime = timeCreated.get('SystemTime')
                timeStamp = datetime.strptime(systemTime[:-9], '%Y-%m-%dT%H:%M:%S.%f').strftime('%d/%m/%y %H:%M:%S,%f')
                binaryEventData =  bytearray.fromhex(binaryEventData.text).decode().replace("'",'"')
                self.cursor.callproc('PRC_INSERT_EELOG',[timeStamp, systemTime, processId, threadID, file, binaryEventData])

            except cx_Oracle.DatabaseError as e:
                error, = e.args
                self.registerLogPrint('insertLog', error)
                self.registerLogPrint('insertLog', [timeStamp, systemTime, processId, threadID, file, binaryEventData])
                '''systemTime = systemTime.replace('00-', str(random.randint(10,99)) + '-')'''

            except Exception as e:
                print(e)

        self.cursor.execute(""" INSERT INTO EELOG_CONTROLE (LOGFILENAME) VALUES ('{}') """.format(file))
        self.connection.commit()
        self.registerLogPrint('insertLog', 'Insert concluido em: ' + str(time.time() - t0))


    def createTable(self):
        try:
            self.registerLogPrint('createTable', "Criando EELOG, EELOG_CONTROLE e PRC_INSERT_EELOG....")
            query_table_eelog = """
                                    CREATE TABLE EELOG (
                                        ID NUMBER GENERATED  ALWAYS AS IDENTITY,
                                        TIMESTAMP TIMESTAMP(9),
                                        SYSTEMTIME VARCHAR(45),
                                        PROCESSID NUMBER,
                                        THREADID NUMBER,
                                        LOGFILENAME VARCHAR(128),
                                        EVENTDATA CLOB,
                                        PRIMARY KEY (SYSTEMTIME)
                                    )

                                """
            query_table_eelog_controle = """
                                            CREATE TABLE EELOG_CONTROLE (
                                                ID NUMBER GENERATED  ALWAYS AS IDENTITY,
                                                TIMESTAMP DATE DEFAULT SYSDATE,
                                                LOGFILENAME VARCHAR(45)
                                            )
                                        """

            query_PRC_INSERT_EELOG = """
                                        --- PROCEDURE PARA INSERT EELOG .ETL
                                        CREATE OR REPLACE PROCEDURE PRC_INSERT_EELOG
                                        (
                                            TIMESTAMP IN TIMESTAMP,
                                            SYSTEMTIME IN VARCHAR,
                                            PROCESSID IN NUMBER,
                                            THREADID IN NUMBER,
                                            LOGFILENAME IN VARCHAR,
                                            EVENTDATA IN CLOB
                                        )
                                        IS
                                        BEGIN
                                            INSERT INTO
                                                 EELOG(
                                                     TIMESTAMP,
                                                     SYSTEMTIME,
                                                     PROCESSID,
                                                     THREADID,
                                                     LOGFILENAME,
                                                     EVENTDATA
                                                 )
                                                 VALUES(
                                                     TIMESTAMP,
                                                     SYSTEMTIME,
                                                     PROCESSID,
                                                     THREADID,
                                                     LOGFILENAME,
                                                     EVENTDATA
                                                 );
                                        END;
                                    """
            self.cursor.execute(query_table_eelog)
            self.cursor.execute(query_table_eelog_controle)
            self.cursor.execute(query_PRC_INSERT_EELOG)
            self.connection.commit()
            self.registerLogPrint('createTable', "Criado com sucesso!")
            return True
        except Exception as e:
            self.registerLogPrint('createTable', e)
            return False


def main(ct: bool, il: bool, db_user: str, db_password: str, db_host: str, db_SID: str):

    eelogEQTL = EElogEQTL(db_user=db_user, db_password=db_password,
                          db_dsn="{}/{}".format(db_host, db_SID))
    if ct:
        eelogEQTL.createTable()

    if il:
        listFiles = eelogEQTL.listFilesElt()
        eelogEQTL.registerLogPrint('Main', "Serao processados os arquivos: " + str(listFiles)   )
        list(map(lambda file: eelogEQTL.insertLog(file), listFiles))

    eelogEQTL.closeDB()
    eelogEQTL.closeFileLog()

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
    description="""Registra em banco os arquivos .etl Elipse -- Versão 1.0.0v""",
    prog="EElogEQTL",
    epilog="Ex: EELOGEQTL.exe --il --db_user XXXXXX --db_password XXXXXX --db_host localhost --db_SID XXXXXX"
    )

    parser.add_argument("--ct", action='store_true',
                        help="Cria a tabela EELOG, EELOG_CONTROLE e PROCEDURE PRC_INSERT_EELOG",
                        default=False, required=False)

    parser.add_argument( "--il", action='store_true',
                        help="Insere os logs do diretório corrente no banco de dados",
                        default=False, required=False)

    parser.add_argument("--db_user",
                        help="Usuário do banco de dados",
                        type=str, required=True)

    parser.add_argument("--db_password",
                        help="Senha do do usuário do banco de dados",
                        type=str, required=True)

    parser.add_argument("--db_host",
                        help="Endereço do servidor do banco de dados",
                        type=str, required=True)

    parser.add_argument("--db_SID",
                        help="Nome do banco de dados de dados",
                        type=str, required=True)

    main(**vars(parser.parse_args()))
