import requests
import os
from bs4 import BeautifulSoup
from multiprocessing import Pool

cwd = os.getcwd()


def get_data(component, year, type='XPT'):
    base_url = 'https://wwwn.cdc.gov'
    request_url = 'https://wwwn.cdc.gov/nchs/nhanes/Search/DataPage.aspx?Component=' + component + '&CycleBeginYear=' + str(
        year)
    r = requests.get(request_url)
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
    year = 2001  # 2017 ,2019的数据集还未整理，不能下载
    component='Questionnaire' # except Limited Access Data 受限数据需要审批通过下载
    get_data(component, year, type='XPT')
    print('All download finished!')
