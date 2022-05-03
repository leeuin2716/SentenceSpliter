import pandas as pd
import re,kss
from tqdm import tqdm

def rmEmoji(inputData):
    return inputData.encode('utf-8', 'ignore').decode('utf-8')
def rmEmoji_ascii(inputString):
    return inputString.encode('ascii', 'ignore').decode('ascii')

def pre(data):
    df= data.copy()
    df['review'].apply(lambda x : re.sub('([♡❤✌❣♥ᆢ✊❤️✨⤵️☺️;”“]+)', '', str(x)))
    df['review']=df['review'].str.replace('^','') 
    df['review']=df['review'].str.replace('~','')
    df['review']=df['review'].str.replace('!','. ')   
    df['review']=df['review'].str.replace('다/','다. ')
    df['review']=df['review'].str.replace('요/','요. ') 
    return df


if __name__=="__main__":


    csv_file_name = '0421_P05_Rip'
    df = pd.read_csv(csv_file_name+'.csv')
    split_data = csv_file_name.split('_') 
    subject,group_num = split_data[-1],split_data[-2]
    data_1 = df[['part_group_id','review_doc_no','review']]
    pre_data = pre(data_1)
    print(pre_data['review'])

    # sun = pd.read_csv('0421_P09_suncare.csv')
    # sun_1 = sun[['part_group_id','review_doc_no','review']]
    # pre_sun=pre(sun_1)

    origin=[]
    origin_col = ['part_group_id','review_doc_no','review','doc_part_no','kth_review']
    origin_df = pd.DataFrame(columns=origin_col)
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

    origin_df.to_csv(f'pre_{group_num}_{subject}_split.csv',index=False,encoding='utf-8-sig')