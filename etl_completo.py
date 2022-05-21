#Imports 
import os
import re
import math
import json
import sqlite3
import logging 
import requests
import pandas as pd
import numpy  as np

from datetime   import datetime
from bs4        import BeautifulSoup
from sqlalchemy import create_engine

# DATA COLLETION 

def data_collection(headers,url):
    #data_collection(headers,url)

    #REQUEST TO URL
    page = requests.get( url, headers=headers )


    #BEAUTIFUL OBJECT
    soup = BeautifulSoup(page.text, 'html.parser')

    # ================================ PRODUCT DATA =========== # 

    products = soup.find( 'ul', class_='products-listing small' )


    #list of products
    products_list= products.find_all ('article',class_="hm-product-item")


    #product id

    product_id = [p.get('data-articlecode') for p in products_list]

    #product category

    products_category= [p.get('data-category') for p in products_list]

    #product name 

    #extraction all the name 
    products_list_2=  products.find_all('a', class_='link')

    products_name =[p.get_text('link') for p in products_list_2]

    #product price

    #extraction informations of price
    products_list_3 = products.find_all('span', class_='price regular')

    #find all the price in page
    products_price= [p.get_text('price_regular') for p in products_list_3]

    data = pd.DataFrame([product_id,products_category,products_name,products_price]).T

    data.columns = ['product_id','products_category','products_name','products_price']

    return data

# DATA COLLECTION BY PRODUCT

def data_collection_composition_by_product(data,headers):

    #empty Data Frame (to insert the joins)
    df_compositions= pd.DataFrame()

    aux= []

    cols= ['Composition','Size','Art. No.','Fit','messages.garmentLength','messages.waistRise',
    'Care instructions',
     'Material',
     'Description',
     'Imported',
     'Concept',
     'Nice to know']
        
    df_pattern = pd.DataFrame( columns=  cols )


    #API Requests
    for i in range(len(data)):
        
        
        url= 'https://www2.hm.com/en_us/productpage.'+ data.loc[i,'product_id'] +'.html'
        logger.debug('Product: %s',url)
    
        page = requests.get( url, headers=headers )

        #Beautifull Souo object
        soup= BeautifulSoup(page.text,'html.parser')

        ### ===================== color name ======================

        products_list= soup.find_all('a',class_='filter-option miniature active') + soup.find_all('a',class_='filter-option miniature') 

        #product id
        products_id = [p.get('data-articlecode') for p in products_list]

        #color name
        color_name = [p.get('data-color') for p in products_list]

        #create data frame

        df_color = pd.DataFrame([products_id,color_name]).T

        #rename the columns of DF
        df_color.columns= ['Art. No.','color_name']
    
    
        for j in range(len(df_color)):
            url= 'https://www2.hm.com/en_us/productpage.'+ df_color.loc[j,'Art. No.'] +'.html'
            logger.debug( 'Color: %s',url ) 
            
        
            page = requests.get( url, headers=headers )

            #Beautifull Souo object
            soup= BeautifulSoup(page.text,'html.parser')
            
            #=========================product name ==============================
            product_name = soup.find_all('h1',class_='primary product-item-headline')
            product_name = product_name[0].get_text()
            
            # ==================== product price ======================= #
            
            product_price= soup.find_all('div',class_='primary-row product-item-price')
            product_price = re.findall(r'\d+?.\d+',product_price[0].get_text())[0]
           

            # ============================== composition =============================== #

            #composition
            composition_list= soup.find_all('div', class_ = 'details-attributes-list-item')
            
            #composition_list = soup.find_all('dd','details-list-item')
            #product_composition = composition_list[3]

            product_composition = [list(filter (None, p.get_text().split('\n') ) ) for p in composition_list]
            

            if product_composition != []:

                #created the dataframe aux
                df_composition= pd.DataFrame(product_composition).T
            
                
                #renome the columns
                df_composition.columns= df_composition.iloc[0]

                #delete the first row and delete the columns that is the empty
                df_composition= df_composition.iloc[1:].fillna(method='ffill')
                

                #remove pocket lining, shell and lining
                df_composition['Composition'] = df_composition['Composition'].replace('Pocket lining:','',regex=True)
                df_composition['Composition'] = df_composition['Composition'].replace('Shell:','',regex=True)
                df_composition['Composition'] = df_composition['Composition'].replace('Lining:','',regex=True)
                df_composition['Composition'] = df_composition['Composition'].replace('Pocket: Cotton 100%','',regex=True)
            
                #Pocket: Cotton 100%',
        
                # garantee the same number of columns
                df_composition = pd.concat( [df_pattern, df_composition], axis=0 )
                
                #product name 
                df_composition['product_name'] = product_name
                df_composition['product_price'] = product_price
                

                #keep new columns if it shows
                aux= aux + df_composition.columns.tolist()

        
                #merge dataframe of color and composition 
                df_composition= pd.merge(df_composition,df_color, how='left', on ='Art. No.')
                


                #all products 
                df_compositions = pd.concat([df_composition,df_compositions], axis=0)  
        
    else:
        None

    #select just columns necessary
    df_compositions= df_compositions[['Art. No.','product_name','product_price','Composition','color_name','Size','Fit',]]

    #rename columns
    df_compositions.rename(columns = {'Art. No.':'product_id','Composition':'composition','Size':'size',
                                      'Fit':'fit'},inplace=True)

    # Join Showroom data + details

    df_compositions['stlye_id'] = df_compositions['product_id'].apply( lambda x: x[:-3] )
    df_compositions['color_id'] = df_compositions['product_id'].apply( lambda x: x[-3:] )

    df_compositions['scrapy_datetime'] = datetime.now().strftime('%Y-%m-%d-%H-%M:%S')

    return df_compositions

# DATA CLEANING 

def data_cleaning(data_product):

    #product id
    df_data = data_product.dropna(subset=['product_id'])

    #product category

    #product name
    df_data['product_name']= df_data['product_name'].str.replace('\n','')
    df_data['product_name']= df_data['product_name'].str.replace('\t','')
    df_data['product_name']= df_data['product_name'].str.replace('  ','')
    df_data['product_name']= df_data['product_name'].str.replace(' ','_').str.lower()

    #products_price

    df_data['product_price'] = df_data['product_price'].astype(float)

    # Fit
    df_data['fit'] = df_data['fit'].str.replace(' ','_').str.lower()

    #product datetime

    #data['scrapy_datetime']= pd.to_datetime(data['scrapy_datetime'])


    #color name
    df_data['color_name']= df_data['color_name'].str.replace(' ','_').str.lower()

    #break composition by comma (separando as composições dos materiais por coluna)

    df1 = df_data['composition'].str.split(',',expand=True).reset_index(drop=True)

    #Cotton| Spandex | Polyester |  Elastomultiester
    #2 etapa
    #definir as colunas (organizando as colunas por máteria-prima)

    #criando Dataframe vazio com o mesmo tamanho do dataframe original para prencher com as respectivas composições
    df_ref = pd.DataFrame(index=np.arange(len(df_data) ),columns=['cotton','spandex','polyester','elastomultiester'])

    #================================ Composition =======================================#

    #cotton
    #pegando a coluna da composição cotton

    #coletando cotton da primeira coluna df1
    df_cotton_0 = df1.loc[df1[0].str.contains('Cotton',na=True),0]
    #rename colum
    df_cotton_0.name = 'cotton'

    #coletando cotton da segunda coluna df1
    df_cotton_1 = df1.loc[df1[1].str.contains('Cotton',na=True),1]
    #rename columns
    df_cotton_1.name ='cotton'

    #combine the columns cotton
    df_cotton = df_cotton_0.combine_first(df_cotton_1)

    #concat tables
    df_ref = pd.concat([df_ref,df_cotton],axis=1)

    #dropando as colunas duplicadas
    df_ref= df_ref.iloc[:,~df_ref.columns.duplicated(keep='last')]

    ###================== polyester ================= ##

    df_polyester_0 = df1.loc[df1[0].str.contains('Polyester',na=True),0]
    df_polyester_0.name = 'polyester'

    df_polyester_1 = df1.loc[df1[1].str.contains('Polyester',na=True),1]
    df_polyester_1.name = 'polyester'

    #combine the columns cotton
    df_polyester = df_polyester_0.combine_first(df_polyester_1)

    #concat tables
    df_ref = pd.concat([df_ref,df_polyester],axis=1)

    #dropando as colunas duplicadas
    df_ref= df_ref.iloc[:,~df_ref.columns.duplicated(keep='last')]

    # =============================== Elastomultiester ============================== #

    df_elastomultiester = df1.loc[df1[1].str.contains('Elastomultiester',na=True),1]
    #rename colum
    df_elastomultiester.name = 'elastomultiester'

    #concat tables
    df_ref = pd.concat([df_ref,df_elastomultiester],axis=1)

    #dropando as colunas duplicadas
    df_ref= df_ref.iloc[:,~df_ref.columns.duplicated(keep='last')]


    # ======================== Spandex ========================================= #

    df_spandex_1 = df1.loc[df1[1].str.contains('Spandex', na=True),1]
    df_spandex_1.name = 'spandex'

    df_spandex_2= df1.loc[df1[2].str.contains('Spandex', na=True),2]
    df_spandex_2.name = 'spandex'

    df_spandex = df_spandex_1.combine_first(df_spandex_2)

    #concat tables
    df_ref = pd.concat([df_ref,df_spandex],axis=1)

    #dropando as colunas duplicadas
    df_ref= df_ref.iloc[:,~df_ref.columns.duplicated(keep='last')]


    #join of combine with product_id
    df_aux= pd.concat([df_data['product_id'].reset_index(drop=True),df_ref.reset_index(drop=True)],axis=1)

    #format composition data

    #cotton
    df_aux['cotton']= df_aux['cotton'].apply(lambda x: int(re.search('\d+',x ).group(0))/100 if pd.notnull(x) else x)

    #spandex
    df_aux['spandex'] =df_aux['spandex'].apply(lambda x: int(re.search('\d+', x).group(0)) /100 if pd.notnull(x) else x)

    #polyester
    df_aux['polyester']= df_aux['polyester'].apply(lambda x: int(re.search('\d+', x).group(0))/100 if pd.notnull(x) else x)

    #elastomultiester
    df_aux['elastomultiester']=df_aux['elastomultiester'].apply(lambda x: int(re.search('\d+', x).group(0)) /100 if pd.notnull(x) else x)


    #======================== final join ==================#


    #escolhendo o produto com maior composição para fazer o join
    df_aux= df_aux.groupby('product_id').max().reset_index().fillna(0)


    #merge the both columns
    df_data = pd.merge(df_data,df_aux, on='product_id', how='left')

    #drop columns
    df_data= df_data.drop('composition',axis=1)

    #drop duplicates

    df_data= df_data.drop_duplicates()

    return df_data

# DATA INSERT

def data_insert(df_data):
    
    #copy the datas
    data_insert = df_data.copy(deep=True)

    #create database connection
    conn = create_engine('sqlite:///database_hm.sqlite',echo=False)

    #insert data
    data_insert.to_sql('vitrine',con=conn, if_exists='append', index=False)

if __name__ == '__main__':
    
    #logging       
    path = '/home/guilherme/Documentos/repos/ds_ao_dev/projeto_webscrapy/'
    
    if not os.path.exists (path + 'Logs'):
        os.makedirs(path + 'Logs')
    
    logging.basicConfig(
            filename= path +'Logs/etl_hm.log',
            level = logging.DEBUG,
            format= '%(asctime)s - %(levelname)s - %(name)s - %(message)s', 
            datefmt = '%Y-%m-%d %H:%M:%S')

    logger = logging.getLogger('webscraping_hm')

    #paraments and condiction 
    #PARAMENTS
    
    #link princial do modelo de roupa que pŕeciso
    url= 'https://www2.hm.com/en_us/men/products/jeans.html'
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36"}
    #headers = {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko)  Chrome/50.0.2661.102 Safari/537.36 OPR/37.0.2178.54'}
    
    
    #data collection
    data = data_collection(headers,url)
    logger.info('data collection done')    
    
    #data collection by product
    data_product = data_collection_composition_by_product(data,headers)
    logger.info('data collection by composition done')
    
    
    #data cleaning
    data_product_cleaned = data_cleaning(data_product)
    logger.info('data product cleaned done')    
    
    #data insertion 
    data_insert(data_product_cleaned)
    logger.info('data insert done')

