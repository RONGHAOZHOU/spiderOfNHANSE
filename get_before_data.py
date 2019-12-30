import requests
import os
from bs4 import BeautifulSoup
from multiprocessing import Pool

cwd = os.getcwd()

def down_file(dataset, types=None):
    base_url = 'https://wwwn.cdc.gov/nchs/'
    request_url = 'https://wwwn.cdc.gov/nchs/nhanes/' + dataset + '/Default.aspx'
    # 如果没有指定要下载的文件类型列表，request页面上以 xpt,dat,csv,zip,txt为后缀名的文件全部加入待下载的链接列表
    if types is None:
        types = ['xpt', 'dat', 'csv', 'zip', 'txt']
        # types = ['xpt', 'dat', 'csv', 'zip']
    r = requests.get(request_url)
    soup = BeautifulSoup(r.content, 'html5lib')
    # 找到所有的a标签
    all_links = soup.findAll('a')
    # 遍历文件类型，看是否有要下载的文件的链接，有的话加入链接列表
    target_links = []
    for type in types:
        target_links.extend([base_url + link['href'][6:] for link in all_links if link['href'].endswith('.' + type)])

    #数据集存放位置
    dataset_dir = os.path.join(cwd, dataset)
    if not os.path.exists(dataset_dir):
        os.mkdir(dataset_dir)
    # 遍历下载所有链接
    for link in target_links:
        file_name = link.split('/')[-1]
        file_path = os.path.join(dataset_dir, file_name)
        print("Downloading file:%s" % file_name)
        r = requests.get(link, stream=True)
        # download started
        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

        print("%s downloaded!\n" % file_name)
    print("All " + str(dataset) + " files downloaded!")
    return


if __name__ == "__main__":
    dataset_list = [ 'nhanes2', 'nhanes1', 'Hhanes',
                      'nhes3','nhes2','nhes1','nhanes3'] #nhanes3的数据在另一个文件里，应该单独处理

    #常规的每个页面的路径 https: // wwwn.cdc.gov / nchs / nhanes / nhes3 / Default.aspx

    p = Pool()  # 创建进程池
    for dataset in dataset_list:
        p.apply_async(down_file, args=[dataset,])

    print('Waiting for all sub processes done....')
    p.close()
    p.join()
    print('All download finished!')
