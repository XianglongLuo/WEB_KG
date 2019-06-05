import url_manager, html_downloader, html_parser
import pickle
import os
import sys
import threading
import time
import urllib
class MyThread(threading.Thread):
    def __init__(self,name):
        threading.Thread.__init__(self)
        self._running = True
        self.name=name
        # print(self.name)
    def terminate(self):
        self._running = False
    def run(self):
        try:
            while urls.has_new_url() and self._running:
                start=time.time()
                LOCK.acquire()
                new_url = urls.get_new_url()
                LOCK.release()
                html_cont = downloader.download(new_url)
                new_urls, _ = parser.parse(html_cont)
                LOCK.acquire()
                urls.add_new_urls(new_urls)
                spend=time.time()-start
                LOCK.release()
                print(f"Thread:{self.name} craw id:{len(urls.old_urls)} URL:{urllib.parse.unquote(new_url).split('/')[-1]} spend:{str(spend)}")
        except:
            print('save state',sys.exc_info())
            pickle.dump(urls, open('urls.bin', 'wb'))


if __name__=='__main__':

    PATH='urls.pkl'
    root_url = 'https://baike.baidu.com/item/%E6%96%87%E6%B1%87%E6%8A%A5'
    LOCK=threading.Lock()
    urls = url_manager.UrlManager()
    downloader = html_downloader.HtmlDownloader()
    parser = html_parser.HtmlParser()
    threads=[]
    count_thread=12
    if os.path.exists(PATH):
        urls=pickle.load(open(PATH,'rb'))
    else:
        urls.add_new_url(root_url)
    length=len(urls.new_urls)
    print(f'build urls,length={length}')
    for i in range(count_thread):
        print(f'build thread {i}...')
        threads.append(MyThread(str(i)))
    try:
        for t in threads:
            t.start()
            t.join()
    except:
        for t in threads:
            t.terminate()
        print('error!', sys.exc_info()[0])
    finally:
        print('finished,saving state')
        pickle.dump(urls, open(PATH, 'wb'))
