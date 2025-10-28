import os
from datetime import datetime
import logging
from time import sleep

import numpy as np
import pandas as pd
import requests as req
import validators
from bs4 import BeautifulSoup
from ratelimit import limits, sleep_and_retry
from pyrate_limiter import Duration, Rate, Limiter, BucketFullException
from tqdm import tqdm

N_REQ = 10
LIMIT_S = 3    # 1000 reqs per 300s, or 10 reqs per 3s. Pretty lenient!
# persec_rate = Rate(N_REQ, LIMIT_S * Duration.SECOND)
# limiter = Limiter

logging.basicConfig(
    format="\n%(asctime)s %(levelname)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

data_key_dict = { # match on the 'id' field
    'award_agency': 'agencyID',
    'award_procurement_id': 'PIID',
    'award_modification_num': 'modNumber',
    'ref_idv_agency': 'idvAgencyID',
    'ref_idv_procurement_id': 'idvPIID',
    'ref_idv_modification_num': 'idvModNumber',
    'date_signed': 'signedDate',
    'date_effective': 'effectiveDate',
    'date_complete': 'awardCompletionDate',
    'date_ult_complete_est': 'estimatedUltimateCompletionDate',
    'date_solicitation': 'solicitationDate',
    'amount_obligated': 'obligatedAmount',
    'amount_obligated_total': 'totalObligatedAmount',
    'amount_base_exercised_options': 'baseAndExercisedOptionsValue',
    'amount_base_exercised_options_total': 'totalBaseAndExercisedOptionsValue',
    'amount_ultimate': 'ultimateContractValue',
    'amount_ultimate_total': 'totalUltimateContractValue',
    'entity_id': 'UEINumber',
    'entity_name': 'vendorName',
    'entity_dba': 'vendorDoingAsBusinessName',
    'cage_code': 'cageCode',
    'entity_street': 'vendorStreet',
    'entity_street_2': 'vendorStreet2',
    'entity_city': 'vendorCity',
    'entity_state': 'vendorState',
    'entity_zip': 'vendorZip',
    'entity_county': 'vendorCountry',
    'entity_county_disp': 'vendorCountryForDisplay',
    'entity_phone': 'vendorPhone',
    'entity_fax': 'vendorFax',
    'entity_congressional_district': 'vendorCongressionalDistrict',
    'product_service_code': 'productOrServiceCode',
    'product_service_desc': 'productOrServiceCodeDescription',
    'principal_naics_code': 'principalNAICSCode',
    'principal_naics_desc': 'NAICSCodeDescription',
    'performance_state': 'placeStateCode',
    'performance_location': 'placeLocationCode',
    'performance_country': 'placeCountryCode',
    'performance_county': 'principalPlaceOfPerformanceCountyName',
    'performance_city': 'principalPlaceOfPerformanceName',
    'performance_congressional_district': 'principalPlaceOfPerformanceCongressionalDistrict',
    'performance_zip': 'placeOfPerformanceZIPCode',
    'performance_zip_ext': 'placeOfPerformanceZIPCode4',
}


@sleep_and_retry
@limits(calls=N_REQ,period=LIMIT_S)
def limit_req(url,headers={}):
    r = req.get(url,headers=headers)
    if r.status_code != 200:
        raise Exception('API response: {}'.format(r.status_code))
    return r

# def limit_req_2(url, headers={}):


def scrape_doge_endpoint(api_root,endpoint_str,params):
    endpoint_json_list = []
    p_scrape = True
    page = 1
    while p_scrape:
        r = req.get(os.path.join(api_root,endpoint_str),params={**params,"page":page})
        _json_list = r.json()['result'][endpoint_str]
        p_scrape = page < r.json()['meta']['pages']
        endpoint_json_list.extend(_json_list)
        page += 1
    df = pd.DataFrame(endpoint_json_list)
    df = df.rename(columns={'description': 'description_doge'})
    return df

def scrape_doge():
    api_root = 'https://api.doge.gov/savings/'
    params = {
        "sort_by": "date",
        "sort_order": "desc",
        "per_page": 500
    }
    contract_df = scrape_doge_endpoint(api_root,'contracts',params)
    grant_df = scrape_doge_endpoint(api_root,'grants',params)
    property_df = scrape_doge_endpoint(api_root,'leases',params)
    return contract_df, grant_df, property_df

def safe_to_dt(dtstr):
    try:
        dt = pd.to_datetime(dtstr)
    except:
        dt = None
    return dt

def df_row_diff(old_df,new_df):
    return pd.concat([old_df,new_df])[new_df.columns].drop_duplicates(keep=False)

def df_row_diff_2(old_df,stub_df):
    new_df = stub_df.copy()
    drop_idx = []
    for idx, row in tqdm(new_df.iterrows()):  # there HAS to be a way to vectorize this...
        match_series = (old_df[stub_df.columns] == row).all(axis=1)
        if match_series.any():
            drop_idx.append(np.arange(len(match_series))[match_series])
            new_df = new_df.drop(idx,axis=0)
    return new_df, drop_idx

def clean_stub_df(df):
    df.columns = [k.lower().replace(' ','_') for k in df.keys()]
    # in-column value replacement
    if 'uploaded_on' in df.keys():
        df['uploaded_dt'] = [safe_to_dt(dts) for dts in df['uploaded_on'].values]
    # column splitting and replacement
    if 'location' in df.keys():
        loc_part_list = [loc.split(', ') for loc in df['location'].values]
        for idx, loc_part_tup in enumerate(loc_part_list):
            city_pred = len(loc_part_tup[1]) == 2
            df.loc[idx,'city'] = loc_part_tup[0]    # city always first
            df.loc[idx,'state'] = loc_part_tup[1] if city_pred else ''
            if len(loc_part_tup) > 2:
                df.loc[idx,'agency'] = loc_part_tup[2] if city_pred else loc_part_tup[1]
    if 'link' in df.keys():
        df.link = df.link.fillna('')
    if 'vendor' in df.keys():
        df.loc[df.vendor == 'N/A','vendor'] = ''
    return df

def parse_fpds_html(fpds_soup):
    data_dict = {}
    for k, qk in data_key_dict.items():
        element = fpds_soup.find('input',id=qk)
        data_dict[k] = element.get('value',default=None) if element is not None else None
        if 'amount' in k and data_dict[k] is not None:
            data_dict[k] = float(str(data_dict[k]).replace('$','').replace(',',''))
    req_desc_element = fpds_soup.find('textarea',id='descriptionOfContractRequirement')
    data_dict['requirement_desc'] = None if req_desc_element is None else req_desc_element.get('text',default=None)
    return data_dict

def log_row_error(mode,dt,req_url):
    if not os.path.exists("./runlog"):
        os.makedirs("./runlog")
    with open(f"./runlog/scrape-{dt}.txt",'a') as lwf:
        print(f"{mode},{dt},{req_url}",file=lwf)

def extend_contract_data(contract_df,dt):
    fpds_df = pd.DataFrame([])
    rh = req.utils.default_headers()
    # this takes about 2s per iteration. Speedup without DOSing the FPDS server?
    for fpds_link in tqdm(contract_df.fpds_link.values):
        if validators.url(fpds_link):
            try:
                r = req.get(fpds_link,headers=rh)
                contract_row_dict = parse_fpds_html(BeautifulSoup(r.content,features="lxml"))
                fpds_df = pd.concat([fpds_df,pd.DataFrame(contract_row_dict,index=[0])],ignore_index=True)
            except:
                log_row_error('contract',dt,fpds_link)
                # logger.error()
                fpds_df = pd.concat([fpds_df,pd.DataFrame([],index=[0])],ignore_index=True)
        else:
            fpds_df = pd.concat([fpds_df,pd.DataFrame([],index=[0])],ignore_index=True)
    return pd.concat([contract_df.reset_index().drop('index',axis=1),fpds_df],axis=1)

def extend_grant_data(grant_df,dt):
    api_root = 'https://api.usaspending.gov/api/v2/awards/'
    usas_df = pd.DataFrame([])
    rh = req.utils.default_headers()
    for link in tqdm(grant_df.link.values):
        if validators.url(link):
            try:
                grant_id = os.path.basename(link)
                usas_req_url = os.path.join(api_root,grant_id)
                r = limit_req(usas_req_url,headers=rh)
                grant_row_df = pd.json_normalize(r.json(),sep='_')
                grant_row_df = grant_row_df.rename(columns={'description': 'description_usas'})
                usas_df = pd.concat([usas_df,grant_row_df],ignore_index=True)
            except:
                log_row_error('grant',dt,usas_req_url)
                usas_df = pd.concat([usas_df,pd.DataFrame([],index=[0])],ignore_index=True)
        else:
            usas_df = pd.concat([usas_df,pd.DataFrame([],index=[0])],ignore_index=True)
    return pd.concat([grant_df.reset_index().drop('index',axis=1),usas_df],axis=1)

def main():
    # contract_df, grant_df, property_df, stub_contract_df, stub_grant_df, stub_property_df = update_doge_data()
    # save_doge_data(contract_df, grant_df, property_df, stub_contract_df, stub_grant_df, stub_property_df)
    pass

if __name__ == '__main__':
    main()
