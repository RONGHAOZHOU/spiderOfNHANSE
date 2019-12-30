import requests
import os
from bs4 import BeautifulSoup
from multiprocessing import Pool

cwd = os.getcwd()

def down_nhanes3_file(dataset='nhanes3',types=None):
    base_url = 'https://wwwn.cdc.gov/nchs/'
    request_url = 'https://www.cdc.gov/nchs/nhanes/nhanes3/DataFiles.aspx'
    # 如果没有指定要下载的文件类型列表，request页面上以 xpt,dat,csv,zip,txt为后缀名的文件全部加入待下载的链接列表
    if types is None:
        types = ['xpt', 'dat', 'csv', 'zip', 'txt']
    r = requests.get(request_url)
    soup = BeautifulSoup(r.content, 'html5lib')
    # 找到所有的a标签
    all_links = soup.findAll('a')
    # 遍历文件类型，看是否有要下载的文件的链接，有的话加入链接列表
    target_links = []
    for type in types:
        target_links.extend([base_url + link['href'][6:] for link in all_links if link['href'].endswith('.' + type)])

    #数据集存放位置
    dataset_dir = os.path.join(cwd,dataset)
    if not os.path.exists(dataset_dir):
        os.mkdir(dataset_dir)
    # 遍历下载所有链接

    for link in target_links:
        dir_name = link.split('/')[-2]  # 子文件夹名称，分类存放，1A,2A,34A等，对应网页的表格
        dir_path = os.path.join(dataset_dir, dir_name)
        if not os.path.exists(dir_path):
            os.mkdir(dir_path)
        p.apply_async(down_file,args=[link,dir_path])

    print('Waiting for all sub processes done....')
    print("All " + str(dataset) + " files downloaded!")
    return

def down_file(link,dir_path):
    file_name = link.split('/')[-1]
    file_path = os.path.join(dir_path, file_name)
    print("Downloading file:%s" % file_name)
    r = requests.get(link, stream=True)
    # download started
    with open(file_path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
    print("%s downloaded!\n" % file_name)

if __name__ == "__main__":
    p = Pool(4)  # 创建进程池
    down_nhanes3_file()
    p.close()
    p.join()
    print('All download finished!')
