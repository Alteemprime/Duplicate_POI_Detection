import pandas as pd
from tqdm import tqdm

#indonesia preprocessing constants
IND_SCHOOL_PRODUCT = {'kb' : ['kb', 'paud'],
                     'tk' : ['tk'],
                     'sd' : ['sd', 'sdn', 'sdit'],
                     'smp' : ['smp', 'smpn', 'smpit'],
                     'sma' : ['sma', 'sman'],
                     'smk' : ['smk', 'smkn'],
                     'ra' : ['ra'],
                     'mi' : ['mi', 'madrasah ibtidaiyah'],
                     'ma' : ['ma', 'madrasah aliyah'],
                     'mts' : ['mts']
                     }

NUMERALS_ROMANIZATION = { '1' : 'i',
                          '2' : 'ii',
                          '3' : 'iii',
                          '4' : 'iv',
                          '5' : 'v',
                          '6' : 'vi',
                          '7' : 'vii',
                          '8' : 'viii',
                          '9' : 'ix',
                          '10': 'x'
                        }

IND_GOVBUILDING_PRODUCT = {'camat' : 'kecamatan',
                           'desa'  : 'kelurahan',
                           'lurah' : 'kelurahan',
                           }

IND_BANK_PRODUCT = {'bri' : '1',
                    'bni' : '2',
                    'bsi' : '3',
                    'bca' : '4',
                    'bjb' : '5',
                    'btn' : '6',
                    'btpn': '7'
                   }

def process_row(row, string_column = 'names.0.name'):
    #function to fill product and or brand column
    #to differentiate which pair need to be kept, which to be disposed
    tokens = row[string_column].lower().split()
    
    detected_product = []
    detected_brand = []
    product_count = 0
    brand_count = 0
    
    row['product'] = None
    row['brand'] = None
    
    if ('civil_service.educational_institution.school' in row['apple_categories.0']):
        for token in tokens:
            for product, product_alias in IND_SCHOOL_PRODUCT.items():                
                if token in product_alias:
                    detected_product.append(product)
                    product_count += 1
                    if product_count > 1:
                        row['product'] = 'multiple'
                        break
                    else:                        
                        row['product'] = detected_product[0] if detected_product else None            
            
            for numeral, numeral_alias in NUMERALS_ROMANIZATION.items():
                if token == numeral_alias:                    
                    detected_brand.append(numeral)
                    brand_count += 1
                    break
                if token.isdigit():
                    detected_brand.append(token.lstrip('0'))
                    brand_count += 1
                    break
                if brand_count > 1 :
                    row['brand'] = 'multiple'
                    break
                else :
                    row['brand'] = detected_brand[0] if detected_brand else None
                    
    if ('civil_service.government_office.government_complex.government_building' in row['apple_categories.0']):
        for i,token in enumerate(tokens):
            for product, product_alias in IND_GOVBUILDING_PRODUCT.items():
                if token == product:
                    row['product'] = product_alias
                if token == 'dinas' and i+1 <= len(tokens):
                    row['product'] = tokens[i+1]
                    break
    
    if ('civil_service.government_office.government_complex.courthouse' in row['apple_categories.0']):
        for token in tokens:
            if token == 'agama' or token == 'negeri':
                row['product'] = token
                break
                
    if ('consumer_sector.financial_service.banking_service' in row['apple_categories.0']):
        for token in tokens :
            for product, product_alias in IND_BANK_PRODUCT.items():
                if token == product:
                    row['product'] = product_alias
                    break        
                    
    return row 

def drop_falseduplicates(df, product_column = 'product', brand_column = 'brand'):
    index_todrop = []
    
    for i in range(0, len(df)-1, 2):
        product_1 = df.iloc[i][product_column]
        product_2 = df.iloc[i + 1][product_column]
        brand_1 = df.iloc[i][brand_column]
        brand_2 = df.iloc[i + 1][brand_column]
        
        if product_1 != product_2 and ((product_1 is not None) and (product_2 is not None)):
            index_todrop.extend([i,i+1])
            continue
        
        if brand_1 != brand_2 and ((brand_1 is not None) and (brand_2 is not None)):
            index_todrop.extend([i, i + 1])
            continue
    
    falseduplicates_df = df.iloc[index_todrop]
    falseduplicates_df.to_csv('detected_falseduplicates.csv', index=False)
    print(f'detected_falseduplicates.csv contain {falseduplicates_df.shape[0] / 2} pair')
    
    trueduplicates_df = df.drop(index= index_todrop).reset_index(drop=True)
    trueduplicates_df.to_csv('detected_trueduplicates.csv', index=False)
    print(f'detected_trueduplicates.csv contain {trueduplicates_df.shape[0] / 2} pair')

if __name__ == "__main__":
    #opens file that contain variable filename based on lev and cosine value user inputted
    with open('fileoutputbuffer_info.txt', 'r') as f:
        file_in = f.read().strip()
    #file_in = "pairoutput_0,7Cosine_0,8Lev.csv"
    df = pd.read_csv(file_in)
    tqdm.pandas()
    df = df.apply(lambda row: process_row(row), axis=1, desc = 'attributing relevant products and brands...')
    drop_falseduplicates(df)
