from datetime import datetime
import pandas as pd
import re,kss,os,time, datetime,multiprocessing
from tqdm import tqdm


def path_create(path):
    try:
        if not os.path.isdir(path):
            os.mkdir(path)
    except OSError:
        print("Error: fail to create path")
    return None

def rmEmoji(inputData):
    return inputData.encode('utf-8', 'ignore').decode('utf-8')
def rmEmoji_ascii(inputString):
    return inputString.encode('ascii', 'ignore').decode('ascii')

def pre(data):
    df = data
    df['review']=df['review'].apply(lambda x : re.sub('([♡❤✌❣♥ᆢ✊❤️✨⤵️☺️;”“]+)', '', str(x)))
    df['review']=df['review'].str.replace('^','',regex=True) 
    df['review']=df['review'].str.replace('~','',regex=True)
    df['review']=df['review'].str.replace('!','. ',regex=True)  
    df['review']=df['review'].str.replace('！','. ',regex=True)  
    df['review']=df['review'].str.replace('다/','다. ',regex=True)
    df['review']=df['review'].str.replace('요/','요. ',regex=True) 
    return df



def runing(csv_file_name):
    '''
    문장분리기 실행 함수 
    인자값 csv_file_name
    csv_file_name  예시 : 0421_P10_skin   파일 형식제외     날짜/그룹번호/이름  나눔자  = '_'   
    원본 파일은 같은 경로 존재 가정
    분리된 파일 경로 -> C:/SplitData/division_data/
    최종 문장분리 파일 경로 -> C:/SplitData/full_data/
    최종 파일 생성 예시 -> pre_{group_num}_{subject}_split_full.csv
    '''
    start_time = time.time()
    path_list = ['C:/SplitData/','C:/SplitData/full_data/','C:/SplitData/division_data/']
    for path_data  in path_list:
        path_create(path_data)
        
    df = pd.read_csv(csv_file_name+'.csv')
    split_data = csv_file_name.split('_') 
    subject,group_num = split_data[-1],split_data[-2]
    org_data = df[['part_group_id','review_doc_no','review']]
    data_length = org_data.shape[0]  # 전체 개수 
    division_cnt = data_length//2500    # 분할개수 확인 
    if division_cnt == 0:
        division_cnt = 1
    list_index = (data_length//division_cnt)  #분할된 리뷰의 개수  
    
    cnt = 0
    split_data_list = []
    for dfint in tqdm(range(1,division_cnt+2),position=0,desc='리뷰 분할'):
        # print(f'총 분할 개수  = {division_cnt} 개 ')
        # print(f'현재 진행중인 분할 번호  = {dfint} 번째 ')
        cnt += list_index
        if dfint == 1:
            splitdata = org_data.iloc[:cnt,:].reset_index(drop=True)
        elif dfint>1 and dfint<division_cnt+1:
            start_idx = (cnt-list_index)
            splitdata = org_data.iloc[start_idx:cnt,:].reset_index(drop=True)
        elif dfint == division_cnt+1:
            start_idx = (cnt-list_index)
            splitdata = org_data.iloc[start_idx:,:].reset_index(drop=True)
        
        split_data_list.append({'splitdata':splitdata,'dfint':dfint,'subject':subject,'group_num':group_num})
        
    mutil_runing(split_data_list)
    data_merge(division_cnt,subject,group_num)    
       
       
    end_time = time.time()
    
    full_time = (end_time-start_time)
    result_time = (str(datetime.timedelta(seconds=full_time)).split("."))[0]
    print(f'문장분리 총 소요 시간  : {result_time} 입니다. ')    
        
        
        
        

def mutil_runing(split_data_list):
    
    '''
    멀티 프로세싱 사용  
    '''
    
    
    pool = multiprocessing.Pool(processes=5)
    pool.map(spliter, split_data_list)
        
    pool.close()
    pool.join()
    
    
    
    
    
    
def spliter(data):
    '''
    문장분리 후 분리된 csv파일 저장
    
    '''
    # splitdata,dfint,subject,group_num
    splitdata = data['splitdata']
    dfint = data['dfint']
    subject = data['subject']
    group_num = data['group_num']
        
        
    pre_data = pre(splitdata)
    print('리뷰개수 :' + str(pre_data.shape[0]))
    

    origin=[]
    origin_col = ['part_group_id','review_doc_no','review','doc_part_no','kth_review']
    origin_df = pd.DataFrame(columns=origin_col)
    idx = 0
    for idx, row in tqdm(pre_data.iterrows(),position=0 ,desc='문장분리중'):
        
        pg = pre_data.iloc[idx,0]
        rvn = pre_data.iloc[idx,1]
        rv = pre_data.iloc[idx,2]
        s_rv = kss.split_sentences(rv)
        
        dcn = 0
        for k in range(len(s_rv)):
            kth_rv = s_rv[k]
            kth = (pg,rvn,rv,k+1,kth_rv)
            origin.append(kth)
        kth_df= pd.DataFrame(origin,columns=origin_col)
    origin_df = pd.concat([origin_df,kth_df])

    origin_df.to_csv(f'C:/SplitData/division_data/pre_{group_num}_{subject}_split_{dfint}.csv',index=False,encoding='utf-8-sig')
        

def data_merge(division_cnt,subject,group_num):
    '''
    분할된 csv파일 가져와 하나의 full파일로 통합 
    '''
    # 분할테이더 합치기  
    full_origin=[]
    full_origin_col = ['part_group_id','review_doc_no','review','doc_part_no','kth_review']
    full_origin_df = pd.DataFrame(columns=full_origin_col)   
    for dfint in range(1,division_cnt+2): 
        divided_df = pd.read_csv(f'C:/SplitData/division_data/pre_{group_num}_{subject}_split_{dfint}.csv')
        full_origin_df = pd.concat([full_origin_df,divided_df])
    full_origin_df = full_origin_df.reset_index(drop=True)
    full_origin_df.to_csv(f'C:/SplitData/full_data/pre_{group_num}_{subject}_split_full.csv',index=False,encoding='utf-8-sig')
        
    
    
    

if __name__=="__main__":
    
    csv_file_name = '0421_P04_maskpack'
    
    runing(csv_file_name)
    
    
    
    