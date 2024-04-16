import requests
from bs4 import BeautifulSoup

def scrape_books():
    url = "http://books.toscrape.com/"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    #TODO

    return book_data