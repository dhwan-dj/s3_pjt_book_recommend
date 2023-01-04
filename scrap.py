import pandas as pd
import numpy as np
import requests
import os, re
import sqlite3
from bs4 import BeautifulSoup

# 탐색할 페이지 수, 페이지당 아이템 개수(size)
page_num = 12
size = 120 #24,40,80,120

# 스크래이핑 정보 저장(리스트)
book_list = []

# 데이터프레임
df = pd.DataFrame(index=range(0,page_num*size), 
                  columns=['type','title','author','date','price','sales','review_num','review_score'])

# 데이터베이스 연결
DATABASE_PATH = os.path.join(os.getcwd(), 'scrape_data.db')
conn = sqlite3.connect(DATABASE_PATH)
cur = conn.cursor()

for page_n in range(1, page_num+1):
    # page에 따라서 정보가 바뀌게 됩니다.
    url = f'http://www.yes24.com/Product/Search?query=%EA%B5%AD%EB%A6%BD%EC%A4%91%EC%95%99%EB%8F%84%EC%84%9C%EA%B4%80%EC%82%AC%EC%84%9C%EC%B6%94%EC%B2%9C&domain=ALL&page={page_n}&size={size}&hashNm=%EA%B5%AD%EB%A6%BD%EC%A4%91%EC%95%99%EB%8F%84%EC%84%9C%EA%B4%80%EC%82%AC%EC%84%9C%EC%B6%94%EC%B2%9C'
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')

    # 현재 페이지에서 find_all 사용
    books = soup.find_all("div", {"class":"itemUnit"})

    # 해당 페이지를 저장/기록해줍니다.
    for seq in range(len(books)):
        # 도서 형태
        if books[seq].find("span", {"class":"gd_res"}) is not None:
            book_type = books[seq].find("span", {"class":"gd_res"}).string
        else:
            break
        
        # 책 이름
        if books[seq].find("a", {"class":"gd_name"}) is not None:
            book_name = books[seq].find("a", {"class":"gd_name"}).string
        else:
            break
        
        # 저자명
        if books[seq].find("span", {"class":"authPub info_auth"}).find("a") is not None:
            book_author = books[seq].find("span", {"class":"authPub info_auth"}).find("a").string
        else:
            book_author = books[seq].find("span", {"class":"authPub info_auth"}).string
            book_author = re.sub(r"[^가-힣]", "", book_author)
            
        # 출판일
        if books[seq].find("span", {"class":"authPub info_date"}) is not None:
            book_date = books[seq].find("span", {"class":"authPub info_date"}).string
        else:
            break
        
        # 가격
        if books[seq].find("div", {"class":"info_row info_price"}) is not None:
            book_price = books[seq].find("em", {"class":"yes_b"}).string
            book_price = re.compile(r'[\n\t]').sub('', book_price)
            book_price = re.findall(r'\d+', book_price)
            book_price = int(''.join(book_price))
        else:
            book_price = 0
        
        # 판매지수, 없는 경우 0으로 지정해준다.
        if books[seq].find("span", {"class":"saleNum"}) is not None:
            book_sales = books[seq].find("span", {"class":"saleNum"}).string
            book_sales = re.compile(r'[\n\t]').sub('', book_sales)
            book_sales = re.findall(r'\d+', book_sales)
            book_sales = int(''.join(book_sales))
        else:
            book_sales = 0
        
        # 리뷰수
        if books[seq].find("span", {"class":"rating_rvCount"}) is not None:
            review_num = int(books[seq].find("span", {"class":"rating_rvCount"}).find("em", {"class":"txC_blue"}).string)
            #review_num = (re.findall(r'\d+', review_num))
        else:
            review_num = 0
        
        # 평점    
        if books[seq].find("span", {"class":"rating_grade"}) is not None:
            review_score = float(books[seq].find("span", {"class":"rating_grade"}).find("em", {"class":"yes_b"}).string)
            #review_score = (re.findall(r'\d+', review_score))
        else:
            review_score = 0
        
        index=(page_n-1)*size + seq
        print(index, book_type, book_name, book_author, book_date, book_price, book_sales, review_num, review_score)
        
        df.iloc[index]['type']=book_type
        df.iloc[index]['title']=book_name
        df.iloc[index]['author']=book_author
        df.iloc[index]['date']=book_date
        df.iloc[index]['price']=book_price
        df.iloc[index]['sales']=book_sales
        df.iloc[index]['review_num']=review_num
        df.iloc[index]['review_score']=review_score

        # 해당 페이지의 모든 도서 각각의 정보를 딕셔너리로 저장하는 리스트
        book_list.append({"id": index,
                          "type": book_type,
                          "title": book_name,
                          "author": book_author,
                          "date": book_date,
                          "price": book_price,
                          "sales": book_sales,
                          "review_num": review_num,
                          "review_score": review_score
                          })
        
# 데이터프레임 Local 저장
#df.to_excel(r"C:\Users\kdh\Desktop\yes_book.xlsx")
df.to_csv('yes_book.csv', encoding='utf-8-sig')

# 데이터베이스 초기화 및 도서정보 저장
create_table = """CREATE TABLE books (
                        id INTEGER,
                        type TEXT,
                        title TEXT,
                        author TEXT,
                        date TEXT,
                        price INTEGER,
                        sales INTEGER,
                        review_num INTEGER,
                        review_score FLOAT,
                        PRIMARY KEY (id)
                        );"""
drop_table_if_exists = "DROP TABLE IF EXISTS books;"

cur.execute(drop_table_if_exists)
cur.execute(create_table)

for i in range(0, len(book_list)): 
    cur.execute(
        """INSERT INTO books VALUES(:id, :type, :title, :author, :date, :price, :sales, :review_num, :review_score);"""
        , (book_list[i].get('id'), book_list[i].get('type'), book_list[i].get('title'), book_list[i].get('author'), book_list[i].get('date'),
           book_list[i].get('price'), book_list[i].get('sales'), book_list[i].get('review_num'), book_list[i].get('review_score'))
        )
    
conn.commit()
cur.close()