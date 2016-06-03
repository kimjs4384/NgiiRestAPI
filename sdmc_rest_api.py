# -*- coding: utf-8 -*-
from flask import Flask
from flask import request
from flask import Response
from flask import render_template
import urllib2
import logging
import os
import psycopg2
import tempfile,shutil,zipfile
from subprocess import check_output

# 로깅 모드 설정
logging.basicConfig(level=logging.INFO)
crr_path = os.path.dirname(os.path.realpath(__file__))
LOG_FILE_PATH = os.path.join(crr_path, "..//GeoCoding.log")

# 로깅 형식 지정
# http://gyus.me/?p=418
logger = logging.getLogger("failLogger")
formatter = logging.Formatter('[%(levelname)s] %(asctime)s > %(message)s')
fileHandler = logging.FileHandler(LOG_FILE_PATH)
fileHandler.setFormatter(formatter)
logger.addHandler(fileHandler)

# 한글로 된 인자들을 받을때 오류가 생기지 않게 기본 문자열을 utf-8로 지정
#  http://libsora.so/posts/python-hangul/
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

# TODO: conf 파일 이용 ?!
# DB 연결하기
pgip = '172.22.24.249'
pgport = 5432
pgdb = 'sdmc'
pgaccount = 'ngii'
pgpw = '0000'

conn = psycopg2.connect(database=pgdb, user=pgaccount, password=pgpw, host=pgip, port=pgport)
cur = conn.cursor()

app = Flask(__name__)

# 저장소 위치
# 파일명 : 위치_레이어명_좌표계_요청일시.zip
save_dir = u'/Users/jsKim-pc/Desktop'
# save_dir = u'/home/ngii/app_sw/tomcat-app/webapps/sdmc/download'
temp_dir = tempfile.gettempdir()

@app.route("/test", methods=['GET'])
@app.route("/sdmc/test", methods=['GET'])
def test():
    return 'TEST SUCCESS'

# TODO: 진짜 요청으로 바꾸기
@app.route("/api", methods=['GET'])
@app.route("/sdmc/api", methods=['GET'])
def getRequestData():
    try:
        # 주문 ID 와 파일명을 받아옴
        # TODO: 진짜 데이터를 받는 것으로 변환
        order_id = 'test_id' # request.args.get('order_id', 'auto')
        file_name = None # request.args.get('file_name', 'auto')

        if file_name != None:
            # 파일명이 있을때 파일이 실재로 존재하는지 조회
            if not checkExistResFile(order_id,file_name):
                return "파일 없음"
            return "파일 있음"

        # 파일 생성하기
        makeData(order_id)

        # sql = "select column_name from information_schema.columns " \
        #       "where table_schema = 'nfsd' and table_name = 'nf_a_b01000_test' and ordinal_position = 3"
        # cur.execute(sql)
        # result = cur.fetchone()
        #
        # colum_name = result[0]
        #
        # conn.close()
        #
        return 'testing'

    except Exception as err:
        logger.error(err)
        return "[ERROR] {}".format(err)

# 이전에 생성된 파일이 존재하는지 검사
def checkExistResFile(order_id,file_name):
    # 파일이 존재하면 True, 없으면 False
    if os.path.exists(file_name):
        return True

    return False

# 데이터 생성
def makeData(order_id):
    # 데이터에 생성에 필요한 정보들 가져오기
    # 요청 데이터(들)
    res_layers = getResLayers(order_id)
    # 영역
    res_geom = getResGeom(order_id)
    # 포멧
    res_formats = getResFormats(order_id)
    # 기준 일시
    res_date = getResDate(order_id)
    # 좌표계
    res_srs = getResSrs(order_id)

    # sql 만들어 놓기
    # TODO: 시간 검사 추가
    sql_frame = u"select {} from nfsd.{} where bdid is not NULL"

    # 임시 폴더 생성 / 디렉토리 저장
    temp_folder = os.path.join(temp_dir, next(tempfile._get_candidate_names()))
    os.mkdir(temp_folder)

    # 윈도우가 아닌 경우 PATH 추가
    ogr2ogrPath = None
    if sys.platform == "win32":
        ogr2ogrPath = ""
    else:
        ogr2ogrPath = "/Library/Frameworks/GDAL.framework/Versions/1.11/Programs/"

    # 데이터 생산
    for res_layer in res_layers:
        # 기본(export)할 column list 만들기
        ext_col_list = getExtColumns(res_layer)
        sql = sql_frame.format(ext_col_list, res_layer)

        # TODO: 원본 좌표계와 GeoPackage 추가
        command = u'{}ogr2ogr ' \
                  u' --config SHAPE_ENCODING UTF-8 -f "ESRI Shapefile" {}.shp ' \
                  u'-t_srs EPSG:{} PG:"host={} user={} dbname={} password={}" -sql "{}"' \
            .format(ogr2ogrPath, os.path.join(temp_folder,res_layer),res_srs, pgip, pgaccount, pgdb, pgpw, sql)
        rc = check_output(command.decode(), shell=True)

        with open(os.path.join(temp_folder, '{}.cpg'.format(res_layer)), "w") as cpgFile:
            cpgFile.write('UTF-8')

    # 만들어진 데이터 저장소에 ZIP 압축
    shutil.make_archive(os.path.join(save_dir,"{}_{}_{}_{}.zip".format(res_geom,res_layers[0],res_srs,res_date)),
                        'zip',temp_folder)

    # 임시 폴더 삭제
    shutil.rmtree(temp_folder)

# TODO: 세부 로직 구현(레이어)
# 신청한 자료 목록(레이어명)
# table : odr_prd.product_info
# column : layer_id
def getResLayers(order_id):
    return [u'nf_a_b01000_test']

# TODO: 세부 로직 구현(영역)
# 신청한 자료의 영역
# table : odr_prd.order_info
# column : order_type + bjcd / map_num / map_name / shape
def getResGeom(order_id):
    return u'수원시'

# TODO: 세부 로직 구현(포멧)
# 신청한 자료의 포멧
# table : odr_prd.order_info
# column : format
def getResFormats(order_id):
    return u'shp'

# TODO: 세부 로직 구현(요청 일시)
# 신청한 자료의 기준 일시
# table : odr_prd.order_info
# column : order_date
def getResDate(order_id):
    return u'20151225'

# TODO: 세부로직 구현(좌표계)
# 신청한 자료의 좌표계
# table : odr_prd.order_info
# column : srs
def getResSrs(order_id):
    return 4326

def getExtColumns(res_layer):
    # TODO: 지도 고시일, 준공일
    not_ext_cols = ['create_dttm','delete_dttm']
    ext_col_array = []

    sql = u"select column_name from information_schema.columns " \
          u"where table_schema = 'nfsd' and table_name = '{}' order by ordinal_position asc" \
        .format(res_layer)
    cur.execute(sql)
    results = cur.fetchall()
    for result in results:
        ext_col_array.append(result[0]) # column 리스트를 배열로 만듦

    # export 하지 않을 column 제거
    for not_col in not_ext_cols:
        if not_col in ext_col_array:
            ext_col_array.remove(not_col)

    ext_col_list = ",".join(ext_col_array)

    return ext_col_list


#############################
# 서비스 실행
if __name__ == '__main__':
    app.run()

# 기본 서비스로 실행시 flask 한 프로세스 당 1 요청만 처리할 수 있어 성능에 심각한 문제
# http://stackoverflow.com/questions/10938360/how-many-concurrent-requests-does-a-single-flask-process-receive

# Apache WSGI로 실행 필요
# http://flask-docs-kr.readthedocs.org/ko/latest/ko/deploying/mod_wsgi.html
# http://flask.pocoo.org/docs/0.10/deploying/mod_wsgi/
"""
### httpd.conf
# Call Python GeoCoding module by WSGI
LoadModule wsgi_module modules/mod_wsgi-py27-VC9.so
<VirtualHost *>
    ServerName localhost
    WSGIScriptAlias /sdmc d:\www_python\GRestApi\Gsdmc_rest_api.wsgi
    <Directory d:\www_python\GRestApi>
        Order deny,allow
        Require all granted
    </Directory>
</VirtualHost>
"""
