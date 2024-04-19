import scrapy
import os
import requests
import json
import s3fs
from datetime import datetime

class ppidSpider(scrapy.Spider):
    name = 'run'
    start_urls = ['https://ppid.ekon.go.id/']


    def save_json(self, data, filename):
        with open(filename, 'w') as f:
            json.dump(data, f)


    def upload_to_s3(self, local_path, raw_path):
        client_kwargs = {
            'key': '',
            'secret': '',
            'endpoint_url': '',
            'anon': False
        }
        
        s3 = s3fs.core.S3FileSystem(**client_kwargs)
        s3.upload(rpath=raw_path, lpath=local_path)

        if s3.exists(raw_path):
            self.logger.info('File upload successfully')
        else:
            self.logger.info('File upload failed')

    def parse(self, response):  
        select = response.css('ul.navbar-nav li.nav-item')[4]
        link_page = select.css('ul.dropdown-menu li.dropdown-submenu')
        for item in link_page:
            li_text = item.css('a.dropdown-toggle::text').get()
            links_to_page = item.css('ul li a::attr(href)').getall()
            for link in links_to_page:
                yield response.follow(link, callback=self.parse_page, meta={'li_text': li_text})


    def parse_page(self, response):
        content = response.css('div#print p::text').getall()
        content_clean = [text.replace('\n', '').replace('\r', '').replace('\u00a0', '') for text in content]
        tahun = response.url.split('/')[-1].split('-')[1]
        pdf_links = response.css('div.download select.form-control option:nth-of-type(2)::attr(value)').get()
        pdf_links_clean = pdf_links.replace(' ', '%20')
        pdf_link_json = pdf_links_clean.replace('%20', '_').replace('https://ppid.ekon.go.id/source/laporan/', '')
        li_text = response.meta['li_text']
        for link in pdf_links_clean:
            # yield response.follow(link, callback=self.parse_pdf, meta={'content': content})
            pass


        filename = f'{li_text}_{tahun}.json'
        path_raw = 'data_raw'
        dir_raw = os.path.join(path_raw,)
        os.makedirs(dir_raw, exist_ok=True)
        local_path = f'D:/Visual Studio Code/Work/ppid.ekon.go.id/ppidSpider/ppidSpider/data_raw/{filename}'
        # local_path = 'D:\Visual Studio Code\Work\ppid.ekon.go.id\ppidSpider\ppidSpider\data_raw'
        s3_path = f's3://ai-pipeline-statistics/data/data_raw/data_report-goverment/Kementerian Koordinator Bidang Perekonomian/Laporan/{li_text}/{tahun}/json/{filename}'
        s3_path_pdf = f's3://ai-pipeline-statistics/data/data_raw/data_report-goverment/Kementerian Koordinator Bidang Perekonomian/Laporan/{li_text}/{tahun}/pdf/{pdf_link_json}'
        data_json = {
            'link' : response.url,
            'domain' : 'ppid.ekon.go.id',
            'tag' : [
                'ppid.ekon.go.id',
                'Laporan',
                li_text
            ],
            'crawling_time' : datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'crawling_time_epoch' : int(datetime.now().timestamp()),
            'path_data_raw' : s3_path,
            'path_data_clean' : None,
            'title' : li_text,
            'content' : content_clean,
            'file_link' : pdf_links_clean,
            'filename_pdf' : pdf_link_json,
            'path_pdf' : s3_path_pdf,
        }
        self.save_json(data_json, os.path.join(dir_raw, filename))
        self.upload_to_s3(local_path, s3_path.replace('s3://', '') )

    # def parse_pdf(self, response):
    #     url = response.url
    #     path_pdf_raw = 'pdf'
    #     dir_pdf_raw = os.path.join(path_pdf_raw)
    #     save_path = dir_pdf_raw
    #     os.makedirs(save_path, exist_ok=True)

    #     file_name = url.split('/')[-1].replace('%20', '_')
    #     response = requests.get(url)

    #     with open(os.path.join(save_path, file_name), 'wb') as f:
    #         f.write(response.content)
            

        