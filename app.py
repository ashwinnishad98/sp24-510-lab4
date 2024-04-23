import os

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from db import Database

load_dotenv()

st.set_page_config(layout="wide")


def split_frame(input_df, rows):
    """
    Splits a DataFrame into smaller frames of specified number of rows.

    Args:
        input_df (pandas.DataFrame): The input DataFrame to be split.
        rows (int): The number of rows in each split frame.

    Returns:
        list: A list of smaller DataFrames, each containing 'rows' number of rows.
    """
    if input_df.empty:
        return []
    return [input_df.iloc[i : i + rows] for i in range(0, len(input_df), rows)]


def scrape_books():
    """
    Scrapes book data from a website and returns a list of dictionaries containing book information.

    Returns:
        list: A list of dictionaries, where each dictionary represents a book and contains the following keys:
            - "Title": The title of the book.
            - "Price": The price of the book.
            - "Description": The description of the book.
            - "Rating": The rating of the book.
    """
    base_url = "http://books.toscrape.com/catalogue/"
    book_data = []
    progress_bar = st.progress(0)

    with st.spinner("Scraping book data..."):
        for page_number in range(1, 51):
            url = f"{base_url}page-{page_number}.html"
            response = requests.get(url)
            soup = BeautifulSoup(response.content, "html.parser")

            # extract book data from the HTML using BeautifulSoup
            books = soup.find_all("article", class_="product_pod")

            for book in books:
                title = book.h3.a["title"]
                price = book.find("p", class_="price_color").text
                price = float(
                    price.strip("£")
                )  # convert to float after stripping the pound symbol
                rating = book.find("p", class_="star-rating")["class"][1]
                book_url = book.h3.a["href"]
                response = requests.get(base_url + book_url)
                soup_url = BeautifulSoup(response.text, "html.parser")
                description_element = soup_url.select_one("#product_description + p")
                if description_element:
                    description = description_element.text.strip()
                else:
                    description = "No description available"

                book_data.append(
                    {
                        "Title": title,
                        "Price": price,
                        "Description": description,
                        "Rating": rating,
                    }
                )
            progress_bar.progress(page_number / 50)
    progress_bar.empty()
    return book_data


def main():
    """
    Main function that runs the Books Scraper application.

    This function scrapes book data from http://books.toscrape.com/,
    stores the data in a database, and provides a user interface to
    search and sort the book data.

    Returns:
        None
    """
    st.title("Books Scraper")
    st.write(
        "This is a simple web scraper that scrapes book data from http://books.toscrape.com/"
    )
    with Database(os.getenv("DATABASE_URL")) as db_obj:

        db_obj.create_table()

        if not db_obj.check_db_empty():
            book_data = scrape_books()
            for book in book_data:
                db_obj.insert_book(book)

        title_search = st.text_input("Search for a book within the database")

        if title_search:
            df = db_obj.query_books(title_search)
        else:
            df = pd.read_sql(
                "SELECT title, price, rating, description FROM books", db_obj.con
            )

        order_by = st.selectbox(
            "Order by",
            [
                "Rating Low to High",
                "Rating High to Low",
                "Price Low to High",
                "Price High to Low",
            ],
            index=0,
        )
        if not df.empty:
            if order_by == "Rating Low to High":
                df = df.sort_values(by="rating", ascending=False)
            elif order_by == "Rating High to Low":
                df = df.sort_values(by="rating", ascending=True)
            elif order_by == "Price Low to High":
                df = df.sort_values(by="price", ascending=True)
            elif order_by == "Price High to Low":
                df = df.sort_values(by="price", ascending=False)

        container = st.container()

        bottom_menu = st.columns((4, 2, 1))

        with bottom_menu[2]:
            batch_size = st.selectbox("Page Size", options=[25, 50, 100])

        with bottom_menu[1]:
            total_pages = (
                int(len(df) / batch_size) if int(len(df) / batch_size) > 0 else 1
            )
            current_page = st.number_input(
                "Page", min_value=1, max_value=total_pages, step=1
            )

        with bottom_menu[0]:
            st.markdown(f"Page **{current_page}** of **{total_pages}** ")

        pages = split_frame(df, batch_size)

        if current_page <= len(pages):
            container.dataframe(data=pages[current_page - 1], use_container_width=True)


if __name__ == "__main__":
    main()
