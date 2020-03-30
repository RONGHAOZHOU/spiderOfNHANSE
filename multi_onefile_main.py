import requests
import os
from bs4 import BeautifulSoup
from multiprocessing import Pool

cwd = os.getcwd()


def get_data(component, year, type='XPT'):
    base_url = 'https://wwwn.cdc.gov'
    request_url = 'https://wwwn.cdc.gov/nchs/nhanes/Search/DataPage.aspx?Component=' + component + '&CycleBeginYear=' + str(
        year)
    headers = {
        'Connection': 'keep-alive',
        # 'Cache-Control': 'max-age=0',
        # 'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36',
        # 'Sec-Fetch-User': '?1',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'navigate',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Referer': request_url,
        # 'Cookie': 's_fid=2203BBC70C392479-27D076C0CDBCB114; _ga=GA1.2.498240719.1577435849; _gid=GA1.2.311715122.1579177690; TS0196e5be=015d0abe87290b6120acaab07a6a91d10b44c962cef751cd85114ebabc248a79771a0b4a904a45300cae67d60bfeb934ffd63cf5fb; gpv_c54=https%3A%2F%2Fwww.cdc.gov%2Fnchs%2Fnhanes%2F; s_vnum=1580486400419%26vn%3D2; s_invisit=true; s_lv_s=Less%20than%201%20day; s_visit=1; s_ppvl=%5B%5BB%5D%5D; gpv_v45=NHANES%20-%20National%20Health%20and%20Nutrition%20Examination%20Survey%20Homepage; s_cc=true; _gat_GSA_ENOR0=1; s_lv=1579191452441; s_tps=8; s_pvs=12; s_ppv=NHANES%2520-%2520National%2520Health%2520and%2520Nutrition%2520Examination%2520Survey%2520Homepage%2C25%2C35%2C969%2C913%2C969%2C1920%2C1080%2C1%2CP; s_ptc=0.01%5E%5E0.00%5E%5E0.01%5E%5E0.13%5E%5E0.83%5E%5E0.14%5E%5E9.53%5E%5E0.01%5E%5E10.51; _4c_=XVNdb6MwEPwrFQ99KuAPwBCpOiUk16RqmjbhVKkvEWAn%2BEoxAidcrsp%2FPzs4ubYIidnx7K5tdj6srmCVNYA%2BiWAEPT8CKLqx3tihtQYfVsOp%2FuytgYU3PoY52dgpTqntsTCwQz%2F1bZhhol4GcsisG%2BuPrhX6CIQe8VHkHW8sWp1rNIyylm%2BrLzofYhQipeNnWfpt3SMeVOtNZwSXBaJafJVqRknz2kg%2FrF1TqpKFlHU7cN2u65yc5s5W7N0qL1q3KtKKta7qmAvKlBJGDnYCFcu%2FKvJCoGDdCLrL5Voeai3pWHbV0je1QNme52zdcSqLUy4C%2F9mC8W0hNQ1OVWjd6EChjldUdN%2FTDHtJi4JIsYmoE850pgoWq8nDysDhOO7Rz3LXg8d4Ol0lT30wq37vmkOPJ0%2FLHsTi%2FX1XcXm42%2FHTaTVXNKLi%2BbnEbDk%2Bw8nrbHzumzwkpvFdKbK0nLK0PG1eMcNkNV5ekqY9WtVpxVujmM9fjGDEG1mM2YblsjUps8XK5CyepuedLuLFwuTOVgbdi8zkvPK3tEdZI7r2dD36HO%2FsiuhLE1r3crrPVoWN6tc050tsudRHN2NgCDXxPad7z5PlejQZxovHT6Oj9RAiBwniVEy6mdu2mtSjBN37lY0c4AC3DcMA%2BThAYUCA7%2F0YPo9u4fWG01uEAB6NYgJiHCGPRDYiY0CCGMTjUTyC0LsePk9u9XzU2gW%2BAqXI01JvlmnP3A3Xv05%2FRI07Qhj4OHSUcYmHVTk9QUnDt1vWzJkshDKuilPKJRdVWlq9Dz9bkOohU9GbFPWFPho%2FkQgRogyFCFZWkMpDYeAB%2FSjF%2FmJVEIEsz%2FzUJgH1bS%2FzczvEANqEUZwywDD0kHUpCSOMVMkAmpIw7Csej%2F8A'
    }
    r = requests.get(request_url,headers=headers)
    soup = BeautifulSoup(r.content, 'html5lib')
    all_links = soup.findAll('a')
    target_links = [base_url + link['href'] for link in all_links if link['href'].endswith('.' + type)]

    year_dir = os.path.join(cwd, str(year))
    component_dir = os.path.join(year_dir, component)
    # data_dir = os.path.join(component_dir,type)
    data_dir = component_dir
    p = Pool()  # 创建进程池
    for link in target_links:
        if not os.path.exists(year_dir):
            os.mkdir(year_dir)
        # if not os.path.exists(component_dir):
        #     os.mkdir(component_dir)
        if not os.path.exists(data_dir):
            os.mkdir(data_dir)
        p.apply_async(down_file,args=[data_dir, link])
    print('Waiting for all sub processes done....')
    p.close()
    p.join()
    print("All " + str(year) + ' ' + component + ' ' + type + " files downloaded!")
    return

def down_file(data_dir,link):
    file_name = link.split('/')[-1]
    file_path = os.path.join(data_dir, file_name)
    print("Downloading file:%s" % file_name)
    r = requests.get(link, stream=True)
    # download started
    with open(file_path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
    print("%s downloaded!\n" % file_name)

if __name__ == "__main__":
    year = 2015  # 2017 ,2019的数据集还未整理，不能下载
    component='Laboratory' # except Limited Access Data 受限数据需要审批通过下载
    get_data(component, year, type='XPT')
    print('All download finished!')
