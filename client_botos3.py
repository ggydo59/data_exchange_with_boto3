import boto3
from pathlib import Path
import gzip    
import environ
import os
# 환경변수 파일 읽어오기
BASE_PATH=Path(__file__).parent.resolve()
# print(BASE_PATH)
env = environ.Env(DEBUG=(bool, True))
environ.Env.read_env(
    env_file=os.path.join(BASE_PATH, '.env'))


class connection_to_s3:

    def __init__(self, ACCESS_KEY, SECRET_ACCESS_KEY):  
        # s3 client 생성
        try:
            self.s3 = boto3.client('s3',
                aws_access_key_id=ACCESS_KEY,
                aws_secret_access_key=SECRET_ACCESS_KEY)
        except RuntimeError as e:
            print("s3 client 생성에 실패했습니다. 오류: ", e)
    def compressor(self):
        # 압축할 로그파일 경로 생성
        logfile_path = BASE_PATH/env('LOGFILE_PATH')
        logfile_path = logfile_path.resolve() #절대경로 반환
        compressfile_path = BASE_PATH/env('UPLOADFILE_PATH')

        try:
            with open(logfile_path,'rb') as f_in:
                with gzip.open(compressfile_path,'wb') as f_out:
                    f_out.writelines(f_in)  # ./logs/compressed_log.gz 압축 파일 생성
        except RuntimeError as e:
            print("파일 압축에 실패했습니다. 오류내용 : ", e)


    def upload(self, file_name):
        """
        upload the object to s3.

        file_name : file name to upload to s3
        """
        self.compressor() #encrypted_logs파일을 통해 압축된 로그 파일 생성하고 시작
        currentpath = str(BASE_PATH) 
        bucket_name = env('BUCKET_NAME')        
        file_path = currentpath+'/logs/'+file_name
        
        #변수: s3의 버킷명, s3의 저장할 파일명, 로컬에 저장할 파일명
        try:
            self.s3.upload_file(file_path, bucket_name, file_name)
            print(f"{file_name} 업로드가 완료되었습니다.")
        except RuntimeError as e:
            print(f"{file_name} 업로드에 실패했습니다. 오류내용 : ", e)
    def download(self, file_name):
        """
        download the object from s3.
        file_name : file name to download from s3
        """
        try:
            self.s3.download_file(env('BUCKET_NAME'), file_name, file_name)
            print(f"{file_name} 다운로드가 완료되었습니다.")            
        except RuntimeError as e:
            print(f"{file_name} 다운로드에 실패했습니다. 오류내용 : ", e)


if __name__ == '__main__':
   conn = connection_to_s3(env("ACCESS_KEY"), env("SECRET_ACCESS_KEY"))
   conn.download('compressed_log.gz')