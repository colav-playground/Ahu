#!/usr/bin/env python3
import time
import requests
from pymongo import MongoClient


class Ahu:
    def __init__(self, db_name: str = " ",
                 original_collection_name: str = " ",
                 filtered_collection_name: str = " "):
        """
        Constructor to get the database from the Impactu API.

            Parameters:
            -----------
            db_name: str
            Database name.
            By default ImpactU

            original_collection_name: str
            Original collection name.
            By default No_Scholar

            filtered_collection_name: str
            Name of the final collection to implement Moai.
            By default For_Moai
        """
        self.db_name = db_name
        self.original_collection_name = original_collection_name
        self.filtered_collection_name = filtered_collection_name
        self.client = MongoClient("mongodb://localhost")
        self.db = self.client[db_name]
        self.original_collection = self.db[original_collection_name]
        self.filtered_collection = self.db[filtered_collection_name]
        self.server = 'http://impactu.colav.co:8080/api'
        self.max_results = 100

    def fetch_data(self):
        """
        Method to get the raw database from the ImpactU API.
            The resquest URL is
            http://impactu.colav.co:8080/api/affiliation?section=research&tab=products
            The following is obtained:
            {"data": [...]}
            {"count": 100,
            "page" : 1,
            "toral_results" : 203672}

            From these 203672 results a filtering of the products
            that do not have Google Scholar identifier is done.
            Parameters:
            -------
            None

            Return:
            --------
            Collection of filtered products that do not have Google Scholar id.
        """
        page = 1

        while True:
            url_aff = (f"{self.server}/affiliation?"
                       "section=research&tab=products")
            params = {
                "page": page,
                "max": self.max_results
            }

            try:
                response = requests.get(url_aff, params=params)
                response.raise_for_status()
                aff = response.json()
                total_results = aff["total_results"]

                for entry in aff["data"]:
                    if not any(source.get("source") == "scholar"
                               for source in entry.get("external_ids", [])):
                        entry["keep_abstract"] = False  # Borrar el abstract
                        self.original_collection.insert_one(entry)

                if page * self.max_results >= aff["total_results"]:
                    break

                page += 1
                time.sleep(0.1)
                progress = page*self.max_results+400
                print(f"Progress:{progress} of {total_results}")

            except requests.exceptions.RequestException as e:
                print(f"Error in the request: {e}")
                print("Aborting due to a request error")
                break

    def copy_data(self):
        """
        It takes the raw data previously stored in a MongoDB collection,
            performs certain transformations and copies the processed data
            to another MongoDB collection, with the required fields in order
            to implement Moai.

            Parameters:
            -------
            None

            Return:
            --------
            Final collection with the fields needed to implement Moai.
            By default For_Moai
        """
        documents_to_insert = []

        for document in self.original_collection.find():
            abstract = document.get("abstract", "")
            keep_abstract = document.get("keep_abstract", True)
            if not keep_abstract:
                abstract = ""  # Si keep_abstract es False, borra el abstract

            source_name = document.get("source", {}).get("names", {})
            journal = next((entry.get("name", "")
                            for entry in source_name if "name" in entry), "")
            try:
                source_info = document.get("source", {})
                if isinstance(source_info, dict) and \
                        "publisher" in source_info:
                    publisher_info = source_info["publisher"]
                    publisher = publisher_info.get("name", "")
                    country = publisher_info.get("country_code", "")
                else:
                    publisher = ""
                    country = ""
            except AttributeError:
                publisher = ""
                country = ""

            title = document.get('titles', [{}])[0].get('title', "")

            authors_list = document.get("authors", [])[:3]
            formatted_authors = []

            for author in authors_list:
                full_name = author.get("full_name", "")
                full_name = full_name if full_name else ""
                name_parts = full_name.split()
                if len(name_parts) > 1:
                    initials = "".join([name[0] for name in name_parts[:-1]])
                    formatted_name = f"{initials}, {name_parts[-1]}"
                else:
                    formatted_name = full_name
                formatted_authors.append(formatted_name)

            author = " and ".join(formatted_authors)

            year = document.get('year_published', '')
            volume = document['bibliographic_info'].get('volume', '')
            issue = document['bibliographic_info'].get('issue', '')
            start_page = document['bibliographic_info'].get('start_page', '')
            end_page = document['bibliographic_info'].get('end_page', '')
            page = (f"{start_page} - {end_page}" if start_page and end_page
                    else f"{start_page}{end_page}"
                    if start_page or end_page else "")
            lang = document.get('titles', [{}])[0].get('lang', "")

            doi = ''
            pmid = ''
            if 'external_ids' in document:
                for external_ids in document['external_ids']:
                    source = external_ids.get('source', '')
                    if source == 'doi':
                        doi = external_ids.get('id', '')
                    elif source == 'pmid':
                        pmid = external_ids.get('id', '')

            new_document = {
                'journal': journal,
                'publisher': publisher,
                'country': country,
                'title': title,
                'author': author,
                'year': year,
                'volume': volume,
                'issue': issue,
                'page': page,
                'language': lang,
                'abstract': abstract,
                'doi': doi,
                'pmid': pmid
            }

            documents_to_insert.append(new_document)

        if documents_to_insert:
            self.filtered_collection.insert_many(documents_to_insert)
            print(f"Copying from {self.original_collection_name} to"
                  f"{self.filtered_collection_name} completed.")
        else:
            print("No documents were found with 'abstract'"
                  "not empty for copying.")