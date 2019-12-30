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
    for link in target_links:
        if not os.path.exists(year_dir):
            os.mkdir(year_dir)
        # if not os.path.exists(component_dir):
        #     os.mkdir(component_dir)
        if not os.path.exists(data_dir):
            os.mkdir(data_dir)
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
    print("All " + str(year) + ' ' + component + ' ' + type + " files downloaded!")
    return


if __name__ == "__main__":
    year_list = [1999, 2001, 2003, 2005, 2007, 2009, 2011, 2013, 2015]  # 2017 ,2019的数据集还未整理，不能下载
    component_list = ['Demographics', 'Dietary', 'Examination', 'Laboratory',
                      'Questionnaire']  # except Limited Access Data 受限数据需要审批通过下载
    p = Pool()  # 创建进程池
    for year in year_list:
        for component in component_list:
            p.apply_async(get_data, args=(component, year))
    print('Waiting for all sub processes done....')
    p.close()
    p.join()
    print('All download finished!')
